package agentx

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultTimeout 是 WithTimeout 的預設值。
const DefaultTimeout = 5 * time.Second

// Option 調整 Client 的設定。
type Option func(*options)

type options struct {
	logger  *slog.Logger
	timeout time.Duration
}

// WithLogger 指定輸出 log 的 logger，預設是 slog.Default()。
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithTimeout 設定三種等待的上限：Dial 建立連線、送出請求後等 master 回應（ctx 自己
// 有期限時以 ctx 為準），以及每一次寫入。預設 DefaultTimeout，0 表示不設上限。
func WithTimeout(d time.Duration) Option {
	return func(o *options) { o.timeout = max(d, 0) }
}

func buildOptions(opts []Option) options {
	o := options{logger: slog.Default(), timeout: DefaultTimeout}
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

// Client 是一條連到 master（snmpd）的 AgentX 連線，上面可以開多個 Session。
//
// 讀寫失敗或收到無法解析的標頭時，Client 關閉連線、結束所有 Session，把原因存在
// Err，之後 Done 關閉。Client 不會自行重連；要重連就建立新的 Client。
type Client struct {
	conn    net.Conn
	log     *slog.Logger
	timeout time.Duration

	// ctx 在 Client 結束時取消，也是傳給 Handler 的 ctx。
	ctx    context.Context
	cancel context.CancelFunc

	wmu      sync.Mutex // 讓每個 PDU 一次完整寫出，不和其他 goroutine 交錯
	packetID atomic.Uint32

	mu       sync.Mutex
	pending  map[uint32]chan *response // 等待 master 回應的請求，key 是 packetID；結束後為 nil
	sessions map[uint32]*Session       // 開啟中的 session，key 是 sessionID；結束後為 nil
	err      error                     // 結束的原因

	shutdownOnce sync.Once
	handlers     sync.WaitGroup // 執行中的 Handler
	done         chan struct{}
}

// Dial 連到 master。network 是 "unix" 或 "tcp"，例如
// Dial(ctx, "unix", "/var/agentx/master") 或 Dial(ctx, "tcp", "localhost:705")。
func Dial(ctx context.Context, network, address string, opts ...Option) (*Client, error) {
	o := buildOptions(opts)
	d := net.Dialer{Timeout: o.timeout}
	conn, err := d.DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("agentx: %w", err)
	}
	return newClient(conn, o), nil
}

// NewClient 在已經建立好的連線上跑 AgentX。Client 接手 conn，Close 時一併關閉。
func NewClient(conn net.Conn, opts ...Option) *Client {
	return newClient(conn, buildOptions(opts))
}

func newClient(conn net.Conn, o options) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{
		conn:     conn,
		log:      o.logger,
		timeout:  o.timeout,
		ctx:      ctx,
		cancel:   cancel,
		pending:  make(map[uint32]chan *response),
		sessions: make(map[uint32]*Session),
		done:     make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// Close 關閉連線並結束所有 Session，等執行中的 Handler 都返回後才回傳。不需要先關
// Session：連線斷掉時 master 會一併清掉。不可以在 Handler 裡呼叫。
func (c *Client) Close() error {
	c.shutdown(ErrClosed)
	<-c.done
	return nil
}

// Done 在連線結束、所有 Handler 都返回之後關閉。
func (c *Client) Done() <-chan struct{} { return c.done }

// Err 回傳連線結束的原因，連線還在時回傳 nil。本地呼叫 Close 結束的是 ErrClosed；
// master 斷線是包著 io.EOF 的錯誤。
func (c *Client) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.err
}

// shutdown 只執行一次：記下原因、關閉連線、叫醒等待回應的請求、結束所有 Session。
func (c *Client) shutdown(err error) {
	c.shutdownOnce.Do(func() {
		c.mu.Lock()
		c.err = err
		pending, sessions := c.pending, c.sessions
		c.pending, c.sessions = nil, nil
		c.mu.Unlock()

		c.cancel()
		c.conn.Close()
		for _, ch := range pending {
			close(ch)
		}
		for _, s := range sessions {
			s.end(err)
		}
	})
}

// readLoop 是唯一讀取連線的 goroutine，結束時收掉整個 Client。
func (c *Client) readLoop() {
	err := c.receive()
	c.shutdown(err)
	c.handlers.Wait()
	close(c.done)
}

// receive 逐一讀取 PDU 並分派，直到連線出錯。整條連線共用同一個 bufio.Reader，標頭
// 與內容都用 io.ReadFull 讀滿，所以封包分段抵達、或多個 PDU 一起抵達都不會讀錯。
func (c *Client) receive() error {
	r := bufio.NewReader(c.conn)
	var hb [headerSize]byte
	for c.ctx.Err() == nil {
		if _, err := io.ReadFull(r, hb[:]); err != nil {
			return fmt.Errorf("agentx: read: %w", err)
		}
		h, err := parseHeader(hb[:])
		if err != nil {
			return fmt.Errorf("agentx: bad header: %w", err)
		}
		payload := make([]byte, h.payloadLen)
		if _, err := io.ReadFull(r, payload); err != nil {
			return fmt.Errorf("agentx: read: %w", err)
		}
		c.dispatch(h, payload)
	}
	return ErrClosed
}

// write 一次寫出一個完整的 PDU。寫入失敗表示連線已經不能用：結束整個 Client，並回傳
// 結束的原因。
func (c *Client) write(b []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.timeout > 0 {
		c.conn.SetWriteDeadline(time.Now().Add(c.timeout))
	}
	if _, err := c.conn.Write(b); err != nil {
		c.shutdown(fmt.Errorf("agentx: write: %w", err))
		return c.Err()
	}
	return nil
}

// request 送出一個請求並等待 master 回應，op 用在錯誤訊息。ctx 沒有期限時套用
// c.timeout。master 回非零的 res.error 時回傳 *StatusError。
func (c *Client) request(ctx context.Context, op string, h header, body func(*encoder) error) (*response, error) {
	if _, ok := ctx.Deadline(); !ok && c.timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.timeout)
		defer cancel()
	}

	h.flags |= flagNetworkByteOrder
	h.packetID = c.packetID.Add(1)
	e := newEncoder(h)
	if body != nil {
		if err := body(e); err != nil {
			return nil, fmt.Errorf("agentx: %s: %w", op, err)
		}
	}

	ch := make(chan *response, 1)
	c.mu.Lock()
	if c.pending == nil {
		err := c.err
		c.mu.Unlock()
		return nil, err
	}
	c.pending[h.packetID] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, h.packetID)
		c.mu.Unlock()
	}()

	if err := c.write(e.bytes()); err != nil {
		return nil, err
	}
	select {
	case r, ok := <-ch:
		if !ok {
			return nil, c.Err()
		}
		if r.status != StatusNoError {
			return nil, &StatusError{Op: op, Status: r.status, Index: r.index}
		}
		return r, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("agentx: %s: %w", op, ctx.Err())
	}
}

func (c *Client) session(id uint32) *Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.sessions[id]
}

func (c *Client) removeSession(id uint32) *Session {
	c.mu.Lock()
	defer c.mu.Unlock()
	s := c.sessions[id]
	delete(c.sessions, id)
	return s
}
