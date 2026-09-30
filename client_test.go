package agentx

import (
	"bufio"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

var base = OID{1, 3, 6, 1, 4, 1, 99999}

func sub(arcs ...uint32) OID { return append(slices.Clone(base), arcs...) }

const testSessionID = 7

// testData 是 listHandler 的預設內容，依 OID 排序。
func testData() []VarBind {
	return []VarBind{
		{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)},
		{Name: sub(1, 2, 0), Type: TypeOctetString, Value: "hello"},
		{Name: sub(1, 3, 0), Type: TypeCounter64, Value: uint64(1 << 40)},
		{Name: sub(1, 4, 0), Type: TypeTimeTicks, Value: 90 * time.Second},
	}
}

// listHandler 從排序好的清單回答查詢。查到 fail 回傳錯誤，查到 panicOn 直接 panic；
// block 不為 nil 時，GetNext 會等它關閉才回答。
type listHandler struct {
	vbs     []VarBind
	fail    OID
	panicOn OID
	block   chan struct{}
}

func (l *listHandler) Get(_ context.Context, o OID) (VarBind, error) {
	if err := l.trap(o); err != nil {
		return VarBind{}, err
	}
	for _, vb := range l.vbs {
		if vb.Name.Compare(o) == 0 {
			return vb, nil
		}
	}
	return VarBind{Type: TypeNoSuchInstance}, nil
}

func (l *listHandler) GetNext(ctx context.Context, from OID, include bool, to OID) (VarBind, error) {
	if l.block != nil {
		select {
		case <-l.block:
		case <-ctx.Done():
			return VarBind{}, ctx.Err()
		}
	}
	if err := l.trap(from); err != nil {
		return VarBind{}, err
	}
	for _, vb := range l.vbs {
		if c := vb.Name.Compare(from); c > 0 || c == 0 && include {
			if len(to) == 0 || vb.Name.Compare(to) < 0 {
				return vb, nil
			}
			break
		}
	}
	return VarBind{Type: TypeEndOfMIBView}, nil
}

func (l *listHandler) trap(o OID) error {
	if l.panicOn != nil && o.Compare(l.panicOn) == 0 {
		panic("boom")
	}
	if l.fail != nil && o.Compare(l.fail) == 0 {
		return errors.New("backend unavailable")
	}
	return nil
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

func testLogger(t *testing.T) *slog.Logger {
	return slog.New(slog.NewTextHandler(testWriter{t}, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// fakeMaster 在 net.Pipe 的另一端扮演 snmpd。flags 決定它送出的 PDU 用哪種位元組順序。
type fakeMaster struct {
	t      *testing.T
	conn   net.Conn
	r      *bufio.Reader
	flags  uint8
	packet uint32
}

func newPair(t *testing.T, opts ...Option) (*Client, *fakeMaster) {
	t.Helper()
	a, b := net.Pipe()
	c := NewClient(a, append([]Option{WithLogger(testLogger(t))}, opts...)...)
	m := &fakeMaster{t: t, conn: b, r: bufio.NewReader(b), flags: flagNetworkByteOrder}
	t.Cleanup(func() {
		b.Close()
		c.Close()
	})
	return c, m
}

// pdu 組一個 master 送出的 PDU。
func (m *fakeMaster) pdu(typ pduType, sessionID uint32, body func(*encoder)) (header, []byte) {
	m.packet++
	h := header{typ: typ, flags: m.flags, sessionID: sessionID, transactionID: 1000 + m.packet, packetID: m.packet}
	e := newEncoder(h)
	if body != nil {
		body(e)
	}
	return h, e.bytes()
}

func (m *fakeMaster) write(b []byte) {
	m.t.Helper()
	m.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := m.conn.Write(b); err != nil {
		m.t.Fatalf("master write: %v", err)
	}
}

// read 讀一個 client 送來的 PDU。
func (m *fakeMaster) read() (header, []byte) {
	m.t.Helper()
	m.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	var hb [headerSize]byte
	if _, err := io.ReadFull(m.r, hb[:]); err != nil {
		m.t.Fatalf("master read: %v", err)
	}
	h, err := parseHeader(hb[:])
	if err != nil {
		m.t.Fatalf("client sent a bad header: %v", err)
	}
	payload := make([]byte, h.payloadLen)
	if _, err := io.ReadFull(m.r, payload); err != nil {
		m.t.Fatalf("master read payload: %v", err)
	}
	return h, payload
}

// readResponse 讀一個 Response 並解析。
func (m *fakeMaster) readResponse() (header, *response) {
	m.t.Helper()
	h, payload := m.read()
	if h.typ != pduResponse {
		m.t.Fatalf("client sent %s, want Response", h.typ)
	}
	r, err := parseResponse(h, payload)
	if err != nil {
		m.t.Fatalf("client sent a malformed response: %v", err)
	}
	return h, r
}

// respond 回覆 client 送來的請求。
func (m *fakeMaster) respond(req header, sessionID uint32, st Status) {
	m.t.Helper()
	h := header{typ: pduResponse, flags: m.flags, sessionID: sessionID, transactionID: req.transactionID, packetID: req.packetID}
	e := newEncoder(h)
	e.u32(12345) // res.sysUpTime
	e.u16(uint16(st))
	e.u16(0)
	m.write(e.bytes())
}

// roundTrip 送出一個請求並讀回 client 的回應，確認標頭照抄請求。
func (m *fakeMaster) roundTrip(typ pduType, sessionID uint32, body func(*encoder)) *response {
	m.t.Helper()
	h, b := m.pdu(typ, sessionID, body)
	m.write(b)
	rh, r := m.readResponse()
	if rh.packetID != h.packetID || rh.transactionID != h.transactionID || rh.sessionID != h.sessionID {
		m.t.Fatalf("response header %+v does not match request %+v", rh, h)
	}
	if rh.flags != h.flags&flagNetworkByteOrder {
		m.t.Fatalf("response flags %#x, request flags %#x", rh.flags, h.flags)
	}
	return r
}

// ranges 產生 search range list。
func ranges(rs ...searchRange) func(*encoder) {
	return func(e *encoder) {
		for _, r := range rs {
			e.oid(r.start, r.include)
			e.oid(r.end, false)
		}
	}
}

// get 產生 Get 的 search range list（終點為 null）。
func get(oids ...OID) func(*encoder) {
	rs := make([]searchRange, len(oids))
	for i, o := range oids {
		rs[i] = searchRange{start: o}
	}
	return ranges(rs...)
}

func bulk(nonRepeaters, maxRepetitions uint16, rs ...searchRange) func(*encoder) {
	return func(e *encoder) {
		e.u16(nonRepeaters)
		e.u16(maxRepetitions)
		ranges(rs...)(e)
	}
}

func closeBody(reason CloseReason) func(*encoder) {
	return func(e *encoder) {
		e.u8(uint8(reason))
		e.u8(0)
		e.u8(0)
		e.u8(0)
	}
}

// open 在 fake master 上完成 Open 與 Register，並檢查 client 送出的內容。
func open(t *testing.T, c *Client, m *fakeMaster, h Handler) *Session {
	t.Helper()
	type result struct {
		s   *Session
		err error
	}
	done := make(chan result, 1)
	go func() {
		s, err := c.Open(context.Background(), base, "test agent", h)
		if err == nil {
			err = s.Register(context.Background(), sub(1), DefaultPriority)
		}
		done <- result{s, err}
	}()

	oh, op := m.read()
	if oh.typ != pduOpen || oh.sessionID != 0 || oh.flags&flagNetworkByteOrder == 0 {
		t.Fatalf("open header %+v", oh)
	}
	d := newDecoder(oh, op)
	timeout := d.u8()
	d.take(3)
	id, _ := d.oid()
	descr := d.octets()
	if d.err != nil || d.more() || timeout != 0 || id.Compare(base) != 0 || string(descr) != "test agent" {
		t.Fatalf("open payload: timeout %d id %s descr %q err %v", timeout, id, descr, d.err)
	}
	m.respond(oh, testSessionID, StatusNoError)

	rh, rp := m.read()
	if rh.typ != pduRegister || rh.sessionID != testSessionID {
		t.Fatalf("register header %+v", rh)
	}
	d = newDecoder(rh, rp)
	timeout, priority, rangeSubID := d.u8(), d.u8(), d.u8()
	d.u8()
	subtree, _ := d.oid()
	if d.err != nil || d.more() || timeout != 0 || priority != DefaultPriority || rangeSubID != 0 || subtree.Compare(sub(1)) != 0 {
		t.Fatalf("register payload: timeout %d priority %d range %d subtree %s err %v", timeout, priority, rangeSubID, subtree, d.err)
	}
	m.respond(rh, testSessionID, StatusNoError)

	r := <-done
	if r.err != nil {
		t.Fatal(r.err)
	}
	if r.s.ID() != testSessionID {
		t.Fatalf("session ID %d, want %d", r.s.ID(), testSessionID)
	}
	return r.s
}

func checkVarBinds(t *testing.T, r *response, want []VarBind) {
	t.Helper()
	if r.status != StatusNoError {
		t.Fatalf("status %s index %d, want noError", r.status, r.index)
	}
	if len(r.varbinds) != len(want) {
		t.Fatalf("got %d varbinds, want %d: %v", len(r.varbinds), len(want), r.varbinds)
	}
	for i, w := range want {
		g := r.varbinds[i]
		if g.Name.Compare(w.Name) != 0 || g.Type != w.Type || !reflect.DeepEqual(g.Value, w.Value) {
			t.Errorf("varbind %d = %s %s %#v, want %s %s %#v", i+1, g.Name, g.Type, g.Value, w.Name, w.Type, w.Value)
		}
	}
}

func checkError(t *testing.T, r *response, st Status, index uint16) {
	t.Helper()
	if r.status != st || r.index != index || len(r.varbinds) != 0 {
		t.Fatalf("got %s index %d with %d varbinds, want %s index %d with none", r.status, r.index, len(r.varbinds), st, index)
	}
}

func waitDone(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s did not finish", what)
	}
}

// 一次查多個 OID 的 Get：每個 OID 都要有對應的 varbind，名稱就是請求的 OID。
func TestGetMultipleOIDs(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	r := m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0), sub(1, 9, 0), sub(1, 2, 0), sub(1, 4, 0)))
	checkVarBinds(t, r, []VarBind{
		{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)},
		{Name: sub(1, 9, 0), Type: TypeNoSuchInstance},
		{Name: sub(1, 2, 0), Type: TypeOctetString, Value: []byte("hello")},
		{Name: sub(1, 4, 0), Type: TypeTimeTicks, Value: uint32(9000)},
	})
}

func TestGetNextMultipleRanges(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	r := m.roundTrip(pduGetNext, testSessionID, ranges(
		searchRange{start: sub(1, 1, 0)},
		searchRange{start: sub(1, 4, 0)},                    // 最後一個之後：endOfMibView
		searchRange{start: sub(1, 1, 0), end: sub(1, 2)},    // 下一個超出上界：endOfMibView
		searchRange{start: sub(1, 1, 0), include: true},     // 包含起點
		searchRange{start: base, end: sub(1, 1, 0, 1)},      // 上界剛好在第一個之後
		searchRange{start: sub(1, 2, 0), end: sub(1, 2, 0)}, // 空範圍
	))
	checkVarBinds(t, r, []VarBind{
		{Name: sub(1, 2, 0), Type: TypeOctetString, Value: []byte("hello")},
		{Name: sub(1, 4, 0), Type: TypeEndOfMIBView},
		{Name: sub(1, 1, 0), Type: TypeEndOfMIBView},
		{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)},
		{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)},
		{Name: sub(1, 2, 0), Type: TypeEndOfMIBView},
	})
}

// Handler 回傳範圍外的結果時，一律當成 endOfMibView，避免 master 繞圈。
func TestGetNextOutOfRangeResult(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, badNextHandler{})

	r := m.roundTrip(pduGetNext, testSessionID, ranges(searchRange{start: sub(1, 2, 0)}, searchRange{start: sub(1, 2, 0), end: sub(1, 3)}))
	checkVarBinds(t, r, []VarBind{
		{Name: sub(1, 2, 0), Type: TypeEndOfMIBView}, // 回傳起點本身，但 include 為 false
		{Name: sub(1, 2, 0), Type: TypeEndOfMIBView},
	})
}

// badNextHandler 的 GetNext 永遠回傳起點本身。
type badNextHandler struct{}

func (badNextHandler) Get(context.Context, OID) (VarBind, error) {
	return VarBind{Type: TypeNoSuchObject}, nil
}

func (badNextHandler) GetNext(_ context.Context, from OID, _ bool, _ OID) (VarBind, error) {
	return VarBind{Name: from, Type: TypeInteger, Value: 1}, nil
}

func TestGetBulk(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	r := m.roundTrip(pduGetBulk, testSessionID, bulk(1, 3,
		searchRange{start: sub(1, 3, 0)},        // non-repeater
		searchRange{start: sub(1, 2, 0)},        // 第 3 輪結束
		searchRange{start: base, include: true}, // 從頭開始
	))
	checkVarBinds(t, r, []VarBind{
		{Name: sub(1, 4, 0), Type: TypeTimeTicks, Value: uint32(9000)},
		// 第 1 輪
		{Name: sub(1, 3, 0), Type: TypeCounter64, Value: uint64(1 << 40)},
		{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)},
		// 第 2 輪
		{Name: sub(1, 4, 0), Type: TypeTimeTicks, Value: uint32(9000)},
		{Name: sub(1, 2, 0), Type: TypeOctetString, Value: []byte("hello")},
		// 第 3 輪：第一欄結束，名稱沿用上一輪
		{Name: sub(1, 4, 0), Type: TypeEndOfMIBView},
		{Name: sub(1, 3, 0), Type: TypeCounter64, Value: uint64(1 << 40)},
	})
}

// 某一輪全部都是 endOfMibView 之後就停止，不用補滿 max_repetitions。
func TestGetBulkStopsWhenAllEnded(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	r := m.roundTrip(pduGetBulk, testSessionID, bulk(0, 10,
		searchRange{start: sub(1, 3, 0)},
		searchRange{start: sub(1, 4, 0)},
	))
	checkVarBinds(t, r, []VarBind{
		{Name: sub(1, 4, 0), Type: TypeTimeTicks, Value: uint32(9000)},
		{Name: sub(1, 4, 0), Type: TypeEndOfMIBView},
		{Name: sub(1, 4, 0), Type: TypeEndOfMIBView},
		{Name: sub(1, 4, 0), Type: TypeEndOfMIBView},
	})
}

func TestGetBulkEdgeCases(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	// max_repetitions 為 0：只處理 non-repeaters。
	r := m.roundTrip(pduGetBulk, testSessionID, bulk(1, 0, searchRange{start: sub(1, 1, 0)}, searchRange{start: base}))
	checkVarBinds(t, r, []VarBind{{Name: sub(1, 2, 0), Type: TypeOctetString, Value: []byte("hello")}})

	// non_repeaters 比 search range 還多：全部當 non-repeater。
	r = m.roundTrip(pduGetBulk, testSessionID, bulk(9, 5, searchRange{start: sub(1, 1, 0)}))
	checkVarBinds(t, r, []VarBind{{Name: sub(1, 2, 0), Type: TypeOctetString, Value: []byte("hello")}})

	// 沒有 search range。
	r = m.roundTrip(pduGetBulk, testSessionID, bulk(0, 5))
	checkVarBinds(t, r, nil)
}

// 封包一次只到 1 byte，也要完整讀出來。
func TestFragmentedPDU(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	h, b := m.pdu(pduGet, testSessionID, get(sub(1, 1, 0), sub(1, 2, 0)))
	for i := range b {
		m.write(b[i : i+1])
	}
	rh, r := m.readResponse()
	if rh.packetID != h.packetID {
		t.Fatalf("response for packet %d, want %d", rh.packetID, h.packetID)
	}
	checkVarBinds(t, r, []VarBind{
		{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)},
		{Name: sub(1, 2, 0), Type: TypeOctetString, Value: []byte("hello")},
	})
}

// 多個 PDU 在同一次寫入抵達（包含給 client 的 Response 與新的查詢），一個都不能少。
func TestBackToBackPDUs(t *testing.T) {
	c, m := newPair(t)
	s := open(t, c, m, &listHandler{vbs: testData()})

	pingErr := make(chan error, 1)
	go func() { pingErr <- s.Ping(context.Background()) }()
	ph, _ := m.read()
	if ph.typ != pduPing || ph.sessionID != testSessionID {
		t.Fatalf("ping header %+v", ph)
	}

	resp := newEncoder(header{typ: pduResponse, flags: m.flags, sessionID: testSessionID, packetID: ph.packetID})
	resp.u32(0)
	resp.u32(0)
	h1, b1 := m.pdu(pduGet, testSessionID, get(sub(1, 1, 0)))
	h2, b2 := m.pdu(pduGetNext, testSessionID, ranges(searchRange{start: sub(1, 1, 0)}))
	m.write(slices.Concat(resp.bytes(), b1, b2))

	got := map[uint32]*response{}
	for range 2 {
		rh, r := m.readResponse()
		got[rh.packetID] = r
	}
	if err := <-pingErr; err != nil {
		t.Fatalf("ping: %v", err)
	}
	checkVarBinds(t, got[h1.packetID], []VarBind{{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)}})
	checkVarBinds(t, got[h2.packetID], []VarBind{{Name: sub(1, 2, 0), Type: TypeOctetString, Value: []byte("hello")}})
}

// 大量查詢同時在路上，各自在自己的 goroutine 回答，靠 packetID 配對。
func TestManyConcurrentQueries(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	const n = 50
	want := map[uint32]OID{}
	var batch []byte
	for i := range n {
		o := sub(1, uint32(i%4+1), 0)
		h, b := m.pdu(pduGet, testSessionID, get(o))
		want[h.packetID] = o
		batch = append(batch, b...)
	}
	go m.conn.Write(batch)
	for range n {
		rh, r := m.readResponse()
		o, ok := want[rh.packetID]
		if !ok {
			t.Fatalf("unexpected packet %d", rh.packetID)
		}
		delete(want, rh.packetID)
		if r.status != StatusNoError || len(r.varbinds) != 1 || r.varbinds[0].Name.Compare(o) != 0 {
			t.Fatalf("packet %d: %s %v", rh.packetID, r.status, r.varbinds)
		}
	}
}

// Handler 還在執行時，連線照常讀取：Ping 的回應不會被卡住。
func TestSlowHandlerDoesNotBlockReads(t *testing.T) {
	c, m := newPair(t)
	h := &listHandler{vbs: testData(), block: make(chan struct{})}
	s := open(t, c, m, h)

	qh, qb := m.pdu(pduGetNext, testSessionID, ranges(searchRange{start: base}))
	m.write(qb)

	pingErr := make(chan error, 1)
	go func() { pingErr <- s.Ping(context.Background()) }()
	ph, _ := m.read()
	m.respond(ph, testSessionID, StatusNoError)
	if err := <-pingErr; err != nil {
		t.Fatalf("ping while a handler is blocked: %v", err)
	}

	close(h.block)
	rh, r := m.readResponse()
	if rh.packetID != qh.packetID {
		t.Fatalf("response for packet %d, want %d", rh.packetID, qh.packetID)
	}
	checkVarBinds(t, r, []VarBind{{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)}})
}

// master 用 little endian 時，回應也要用 little endian。
func TestLittleEndianMaster(t *testing.T) {
	c, m := newPair(t)
	m.flags = 0
	open(t, c, m, &listHandler{vbs: testData()})

	r := m.roundTrip(pduGet, testSessionID, get(sub(1, 3, 0), sub(1, 2, 0)))
	checkVarBinds(t, r, []VarBind{
		{Name: sub(1, 3, 0), Type: TypeCounter64, Value: uint64(1 << 40)},
		{Name: sub(1, 2, 0), Type: TypeOctetString, Value: []byte("hello")},
	})
}

// 不支援的 PDU 回錯誤，連線照常運作。
func TestUnsupportedPDUs(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	checkError(t, m.roundTrip(pduType(99), testSessionID, func(e *encoder) { e.u32(0) }), StatusParseError, 0)
	checkError(t, m.roundTrip(pduNotify, testSessionID, nil), StatusProcessingError, 0)
	checkError(t, m.roundTrip(pduOpen, 0, nil), StatusProcessingError, 0)
	checkError(t, m.roundTrip(pduIndexAllocate, testSessionID, nil), StatusProcessingError, 0)

	r := m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0)))
	checkVarBinds(t, r, []VarBind{{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)}})
	if err := c.Err(); err != nil {
		t.Fatalf("client ended: %v", err)
	}
}

// payload 解析失敗回 parseError，連線照常運作。
func TestMalformedPayload(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	// OID 宣稱有 5 個 sub-identifier，實際只有 1 個。
	checkError(t, m.roundTrip(pduGet, testSessionID, func(e *encoder) {
		e.buf = append(e.buf, 5, 0, 0, 0)
		e.u32(1)
	}), StatusParseError, 0)
	// TestSet 裡有不認得的 varbind 型別。
	checkError(t, m.roundTrip(pduTestSet, testSessionID, func(e *encoder) {
		e.u16(99)
		e.u16(0)
		e.oid(sub(1, 1, 0), false)
	}), StatusParseError, 0)
	// Close 的 payload 不完整。
	checkError(t, m.roundTrip(pduClose, testSessionID, nil), StatusParseError, 0)

	r := m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0)))
	checkVarBinds(t, r, []VarBind{{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)}})
	if err := c.Err(); err != nil {
		t.Fatalf("client ended: %v", err)
	}
}

func TestUnknownSession(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	checkError(t, m.roundTrip(pduGet, 999, get(sub(1, 1, 0))), StatusNotOpen, 0)
	checkError(t, m.roundTrip(pduCommitSet, 999, nil), StatusNotOpen, 0)
	checkError(t, m.roundTrip(pduPing, 999, nil), StatusNotOpen, 0)
	checkError(t, m.roundTrip(pduClose, 999, closeBody(ReasonShutdown)), StatusNotOpen, 0)
}

func TestNonDefaultContext(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	m.flags |= flagNonDefaultContext
	r := m.roundTrip(pduGet, testSessionID, func(e *encoder) {
		appendOctets(e, "vrf-1")
		get(sub(1, 1, 0))(e)
	})
	m.flags &^= flagNonDefaultContext
	checkError(t, r, StatusUnsupportedContext, 0)
}

// 唯讀：TestSet 回 notWritable；CleanupSet 不回覆。
func TestSetIsRejected(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	r := m.roundTrip(pduTestSet, testSessionID, func(e *encoder) {
		e.varbind(VarBind{Name: sub(1, 1, 0), Type: TypeInteger, Value: 1})
		e.varbind(VarBind{Name: sub(1, 2, 0), Type: TypeOctetString, Value: "x"})
	})
	checkError(t, r, StatusNotWritable, 1)

	// CleanupSet 之後緊接著 Get：下一個讀到的回應必須是 Get 的。
	_, cb := m.pdu(pduCleanupSet, testSessionID, nil)
	m.write(cb)
	r = m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0)))
	checkVarBinds(t, r, []VarBind{{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)}})

	checkError(t, m.roundTrip(pduCommitSet, testSessionID, nil), StatusNoError, 0)
	checkError(t, m.roundTrip(pduUndoSet, testSessionID, nil), StatusNoError, 0)
	checkError(t, m.roundTrip(pduTestSet, testSessionID, nil), StatusNoError, 0)
}

// Handler 回傳錯誤或 panic：回 genErr 與出錯的 search range 位置，client 不受影響。
func TestHandlerFailures(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData(), fail: sub(1, 2, 0), panicOn: sub(1, 3, 0)})

	checkError(t, m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0), sub(1, 2, 0))), StatusGenErr, 2)
	checkError(t, m.roundTrip(pduGet, testSessionID, get(sub(1, 3, 0))), StatusGenErr, 1)
	checkError(t, m.roundTrip(pduGetNext, testSessionID, ranges(searchRange{start: sub(1, 1, 0)}, searchRange{start: sub(1, 3, 0)})), StatusGenErr, 2)
	// GetBulk 第 3 輪從 sub(1,2,0) 往後找時出錯，位置是那一欄（第 2 個 search range）。
	checkError(t, m.roundTrip(pduGetBulk, testSessionID, bulk(1, 5,
		searchRange{start: base},
		searchRange{start: sub(1, 1, 0), include: true},
	)), StatusGenErr, 2)

	r := m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0)))
	checkVarBinds(t, r, []VarBind{{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)}})
	if err := c.Err(); err != nil {
		t.Fatalf("client ended: %v", err)
	}
}

// 值和型別對不上：回 genErr，不 panic。
func TestBadValueFromHandler(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: []VarBind{
		{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(1)},
		{Name: sub(1, 2, 0), Type: TypeInteger, Value: "not a number"},
	}})

	checkError(t, m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0), sub(1, 2, 0))), StatusGenErr, 2)
	checkError(t, m.roundTrip(pduGetNext, testSessionID, ranges(searchRange{start: sub(1, 1, 0)})), StatusGenErr, 1)
}

// master 送 Close：回覆 noError，session 結束並帶出原因，連線本身不受影響。
func TestMasterClosesSession(t *testing.T) {
	c, m := newPair(t)
	s := open(t, c, m, &listHandler{vbs: testData()})

	checkError(t, m.roundTrip(pduClose, testSessionID, closeBody(ReasonByManager)), StatusNoError, 0)
	waitDone(t, s.Done(), "session")
	var sce *SessionClosedError
	if !errors.As(s.Err(), &sce) || sce.Reason != ReasonByManager {
		t.Fatalf("session error = %v", s.Err())
	}
	if err := c.Err(); err != nil {
		t.Fatalf("client ended: %v", err)
	}
	checkError(t, m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0))), StatusNotOpen, 0)
	if err := s.Register(context.Background(), sub(2), DefaultPriority); !errors.As(err, &sce) {
		t.Fatalf("register on a closed session = %v", err)
	}
}

// master 斷線：session 與 client 都結束，等待中的請求被叫醒，錯誤包著 io.EOF。
func TestConnectionLost(t *testing.T) {
	c, m := newPair(t)
	s := open(t, c, m, &listHandler{vbs: testData()})

	pingErr := make(chan error, 1)
	go func() { pingErr <- s.Ping(context.Background()) }()
	m.read()
	m.conn.Close()

	waitDone(t, s.Done(), "session")
	waitDone(t, c.Done(), "client")
	if !errors.Is(c.Err(), io.EOF) {
		t.Fatalf("client error = %v, want io.EOF", c.Err())
	}
	if s.Err() != c.Err() {
		t.Fatalf("session error = %v, client error = %v", s.Err(), c.Err())
	}
	if err := <-pingErr; !errors.Is(err, io.EOF) {
		t.Fatalf("pending ping = %v, want io.EOF", err)
	}
	if _, err := c.Open(context.Background(), base, "again", &listHandler{}); !errors.Is(err, io.EOF) {
		t.Fatalf("open after disconnect = %v", err)
	}
}

// PDU 內容讀到一半斷線：錯誤是 io.ErrUnexpectedEOF。
func TestTruncatedPDU(t *testing.T) {
	c, m := newPair(t)
	open(t, c, m, &listHandler{vbs: testData()})

	_, b := m.pdu(pduGet, testSessionID, get(sub(1, 1, 0)))
	m.write(b[:len(b)-4])
	m.conn.Close()
	waitDone(t, c.Done(), "client")
	if !errors.Is(c.Err(), io.ErrUnexpectedEOF) {
		t.Fatalf("client error = %v, want io.ErrUnexpectedEOF", c.Err())
	}
}

// 標頭無法解析時無法知道下一個 PDU 從哪裡開始：結束連線並回報，不 panic。
func TestBadHeaderEndsConnection(t *testing.T) {
	c, m := newPair(t)
	s := open(t, c, m, &listHandler{vbs: testData()})

	bad := make([]byte, headerSize)
	bad[0] = 2
	m.write(bad)
	waitDone(t, c.Done(), "client")
	if err := c.Err(); err == nil || !strings.Contains(err.Error(), "bad header") {
		t.Fatalf("client error = %v", err)
	}
	if s.Err() != c.Err() {
		t.Fatalf("session error = %v", s.Err())
	}
}

func TestRegisterRejected(t *testing.T) {
	c, m := newPair(t)
	s := open(t, c, m, &listHandler{vbs: testData()})

	errc := make(chan error, 1)
	go func() { errc <- s.Register(context.Background(), sub(2), 100) }()
	rh, _ := m.read()
	m.respond(rh, testSessionID, StatusDuplicateRegistration)

	var se *StatusError
	if err := <-errc; !errors.As(err, &se) || se.Status != StatusDuplicateRegistration || se.Op != "register" {
		t.Fatalf("register = %v", err)
	}
	if s.Err() != nil || c.Err() != nil {
		t.Fatalf("session or client ended: %v / %v", s.Err(), c.Err())
	}
}

// master 不回應：請求逾時；遲到的回應直接丟掉，連線照常運作。
func TestRequestTimeout(t *testing.T) {
	c, m := newPair(t, WithTimeout(200*time.Millisecond))
	s := open(t, c, m, &listHandler{vbs: testData()})

	errc := make(chan error, 1)
	go func() { errc <- s.Ping(context.Background()) }()
	late, _ := m.read()
	if err := <-errc; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ping = %v, want deadline exceeded", err)
	}
	m.respond(late, testSessionID, StatusNoError)

	go func() { errc <- s.Ping(context.Background()) }()
	ph, _ := m.read()
	m.respond(ph, testSessionID, StatusNoError)
	if err := <-errc; err != nil {
		t.Fatalf("second ping: %v", err)
	}
}

func TestSessionClose(t *testing.T) {
	c, m := newPair(t)
	s := open(t, c, m, &listHandler{vbs: testData()})

	errc := make(chan error, 1)
	go func() { errc <- s.Close() }()
	ch, cp := m.read()
	if ch.typ != pduClose || ch.sessionID != testSessionID {
		t.Fatalf("close header %+v", ch)
	}
	if reason, err := parseClose(ch, cp); err != nil || reason != ReasonShutdown {
		t.Fatalf("close reason %s, %v", reason, err)
	}
	m.respond(ch, testSessionID, StatusNoError)
	if err := <-errc; err != nil {
		t.Fatalf("close: %v", err)
	}
	if s.Err() != ErrClosed || c.Err() != nil {
		t.Fatalf("session error %v, client error %v", s.Err(), c.Err())
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
	checkError(t, m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0))), StatusNotOpen, 0)
}

func TestClientClose(t *testing.T) {
	c, m := newPair(t)
	s := open(t, c, m, &listHandler{vbs: testData()})

	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if c.Err() != ErrClosed || s.Err() != ErrClosed {
		t.Fatalf("client error %v, session error %v", c.Err(), s.Err())
	}
	if err := s.Ping(context.Background()); err != ErrClosed {
		t.Fatalf("ping after close = %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("session close after client close = %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second close = %v", err)
	}
}

// Client.Close 會取消 Handler 的 ctx，並等它返回。
func TestClientCloseCancelsHandlers(t *testing.T) {
	c, m := newPair(t)
	h := &listHandler{vbs: testData(), block: make(chan struct{})}
	open(t, c, m, h)

	_, qb := m.pdu(pduGetNext, testSessionID, ranges(searchRange{start: base}))
	m.write(qb)
	go io.Copy(io.Discard, m.conn) // 接住 Handler 被取消後送出的回應

	closed := make(chan struct{})
	go func() {
		c.Close()
		close(closed)
	}()
	waitDone(t, closed, "Close")
}

func TestDial(t *testing.T) {
	path := filepath.Join(t.TempDir(), "master")
	if _, err := Dial(context.Background(), "unix", path); err == nil {
		t.Fatal("dial to a missing socket should fail")
	}

	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	c, err := Dial(context.Background(), "unix", path, WithLogger(testLogger(t)))
	if err != nil {
		t.Fatal(err)
	}
	server := <-accepted
	m := &fakeMaster{t: t, conn: server, r: bufio.NewReader(server), flags: flagNetworkByteOrder}
	open(t, c, m, &listHandler{vbs: testData()})
	r := m.roundTrip(pduGet, testSessionID, get(sub(1, 1, 0), sub(1, 2, 0)))
	checkVarBinds(t, r, []VarBind{
		{Name: sub(1, 1, 0), Type: TypeInteger, Value: int32(42)},
		{Name: sub(1, 2, 0), Type: TypeOctetString, Value: []byte("hello")},
	})
	c.Close()
	server.Close()
}
