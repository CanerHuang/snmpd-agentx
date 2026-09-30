package agentx

import (
	"context"
	"errors"
	"sync"
)

// DefaultPriority 是 RFC 2741 建議的註冊優先權；數字越小越優先。
const DefaultPriority uint8 = 127

// Session 是 Client 上的一個 AgentX session。master 送來屬於這個 session 的查詢，
// 會交給開啟時指定的 Handler。
type Session struct {
	c       *Client
	id      uint32
	handler Handler

	endOnce sync.Once
	done    chan struct{}
	err     error // done 關閉之後才能讀
}

// Open 開啟一個 session。id 與 descr 只用來識別，會出現在 snmpd 的 log 與 AgentX
// MIB 裡。
func (c *Client) Open(ctx context.Context, id OID, descr string, h Handler) (*Session, error) {
	if h == nil {
		return nil, errors.New("agentx: open: nil handler")
	}
	r, err := c.request(ctx, "open", header{typ: pduOpen}, func(e *encoder) error {
		e.u8(0) // o.timeout：0 表示用 master 的預設值
		e.u8(0)
		e.u8(0)
		e.u8(0)
		if err := e.oid(id, false); err != nil {
			return err
		}
		appendOctets(e, descr)
		return nil
	})
	if err != nil {
		return nil, err
	}

	s := &Session{c: c, id: r.sessionID, handler: h, done: make(chan struct{})}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.sessions == nil {
		return nil, c.err
	}
	c.sessions[s.id] = s
	return s, nil
}

// ID 回傳 master 指派的 session ID。
func (s *Session) ID() uint32 { return s.id }

// Register 向 master 註冊 subtree，之後這個範圍內的查詢會轉給 Handler。priority 通常
// 用 DefaultPriority。已經有人註冊同一個 subtree 與優先權時，回傳 Status 為
// StatusDuplicateRegistration 的 *StatusError。
func (s *Session) Register(ctx context.Context, subtree OID, priority uint8) error {
	if err := s.Err(); err != nil {
		return err
	}
	_, err := s.c.request(ctx, "register", header{typ: pduRegister, sessionID: s.id}, func(e *encoder) error {
		e.u8(0) // r.timeout：0 表示用 session 的預設值
		e.u8(priority)
		e.u8(0) // r.range_subid：不註冊範圍
		e.u8(0)
		return e.oid(subtree, false)
	})
	return err
}

// Ping 確認 master 還在回應。收不到回應時，應該關閉 Client 重新連線
// （RFC 2741 7.1.11）。
func (s *Session) Ping(ctx context.Context) error {
	if err := s.Err(); err != nil {
		return err
	}
	_, err := s.c.request(ctx, "ping", header{typ: pduPing, sessionID: s.id}, nil)
	return err
}

// Close 通知 master 關閉 session（reason 為 shutdown）並等待回應。session 已經結束
// 時直接回傳 nil。
func (s *Session) Close() error {
	select {
	case <-s.done:
		return nil
	default:
	}
	_, err := s.c.request(context.Background(), "close", header{typ: pduClose, sessionID: s.id}, func(e *encoder) error {
		e.u8(uint8(ReasonShutdown))
		e.u8(0)
		e.u8(0)
		e.u8(0)
		return nil
	})
	s.c.removeSession(s.id)
	s.end(ErrClosed)
	return err
}

// Done 在 session 結束時關閉：master 送來 Close、連線中斷，或本地關閉。
func (s *Session) Done() <-chan struct{} { return s.done }

// Err 回傳 session 結束的原因，還沒結束時回傳 nil：
//
//   - master 送來 Close：*SessionClosedError
//   - 連線中斷：與 Client.Err 相同
//   - 本地呼叫 Session.Close 或 Client.Close：ErrClosed
func (s *Session) Err() error {
	select {
	case <-s.done:
		return s.err
	default:
		return nil
	}
}

func (s *Session) end(err error) {
	s.endOnce.Do(func() {
		s.err = err
		close(s.done)
	})
}
