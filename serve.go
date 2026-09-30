package agentx

import (
	"context"
	"fmt"
	"runtime/debug"
	"slices"
)

// dispatch 處理讀到的一個 PDU（RFC 2741 7.2.2）。Response 交給等待中的請求；其他
// PDU 除了 CleanupSet 都會回覆。任何問題都只影響這一個 PDU，不中斷連線。
func (c *Client) dispatch(h header, payload []byte) {
	switch h.typ {
	case pduResponse:
		c.deliver(h, payload)

	case pduGet, pduGetNext, pduGetBulk:
		req, hasContext, err := parseRead(h, payload)
		s := c.check(h, err, hasContext)
		if s == nil {
			return
		}
		c.handlers.Add(1)
		go c.serveRead(s, h, req)

	case pduTestSet:
		// 唯讀：有 varbind 就在第一個回 notWritable（RFC 2741 7.2.4.1）。
		vbs, hasContext, err := parseTestSet(h, payload)
		if c.check(h, err, hasContext) == nil {
			return
		}
		if len(vbs) == 0 {
			c.reply(h, StatusNoError, 0)
			return
		}
		c.log.Debug("agentx: rejecting set request", "session_id", h.sessionID, "oid", vbs[0].Name.String())
		c.reply(h, StatusNotWritable, 1)

	case pduCommitSet, pduUndoSet, pduCleanupSet:
		// TestSet 一定失敗，master 不會真的要求 commit；這裡沒有東西要 commit 或 undo。
		// CleanupSet 不回覆（RFC 2741 7.2.4.4）。
		if c.session(h.sessionID) == nil {
			if h.typ != pduCleanupSet {
				c.reply(h, StatusNotOpen, 0)
			}
			return
		}
		if h.typ != pduCleanupSet {
			c.reply(h, StatusNoError, 0)
		}

	case pduPing:
		hasContext, err := parsePing(h, payload)
		if c.check(h, err, hasContext) != nil {
			c.reply(h, StatusNoError, 0)
		}

	case pduClose:
		reason, err := parseClose(h, payload)
		if err != nil {
			c.log.Warn("agentx: malformed PDU", "type", h.typ, "err", err)
			c.reply(h, StatusParseError, 0)
			return
		}
		s := c.removeSession(h.sessionID)
		if s == nil {
			c.reply(h, StatusNotOpen, 0)
			return
		}
		c.log.Info("agentx: session closed by master", "session_id", h.sessionID, "reason", reason)
		c.reply(h, StatusNoError, 0)
		s.end(&SessionClosedError{Reason: reason})

	case pduOpen, pduRegister, pduUnregister, pduNotify,
		pduIndexAllocate, pduIndexDeallocate, pduAddAgentCaps, pduRemoveAgentCaps:
		// 只有 subagent 送給 master 的 PDU。
		c.log.Warn("agentx: unexpected PDU from master", "type", h.typ)
		c.reply(h, StatusProcessingError, 0)

	default:
		// 不認得的 h.type 算解析錯誤（RFC 2741 7.1）。
		c.log.Warn("agentx: unknown PDU type", "type", h.typ)
		c.reply(h, StatusParseError, 0)
	}
}

// check 做 RFC 2741 7.2.2 的共同檢查。通過時回傳對應的 Session；沒通過時已經回覆
// 錯誤給 master，回傳 nil。
func (c *Client) check(h header, parseErr error, hasContext bool) *Session {
	if parseErr != nil {
		c.log.Warn("agentx: malformed PDU", "type", h.typ, "err", parseErr)
		c.reply(h, StatusParseError, 0)
		return nil
	}
	s := c.session(h.sessionID)
	if s == nil {
		c.log.Warn("agentx: PDU for unknown session", "type", h.typ, "session_id", h.sessionID)
		c.reply(h, StatusNotOpen, 0)
		return nil
	}
	if hasContext {
		// 註冊時不帶 context，master 不應該送非預設 context 的查詢過來。
		c.log.Warn("agentx: PDU with non-default context", "type", h.typ)
		c.reply(h, StatusUnsupportedContext, 0)
		return nil
	}
	return s
}

// deliver 把 master 的回應交給等待中的請求。無法解析、或已經沒有人在等的回應直接丟掉
// （RFC 2741 7.2.2）。
func (c *Client) deliver(h header, payload []byte) {
	r, err := parseResponse(h, payload)
	if err != nil {
		c.log.Warn("agentx: dropping malformed response", "packet_id", h.packetID, "err", err)
		return
	}
	c.mu.Lock()
	ch := c.pending[h.packetID]
	delete(c.pending, h.packetID)
	c.mu.Unlock()
	if ch == nil {
		c.log.Debug("agentx: dropping unexpected response", "packet_id", h.packetID)
		return
	}
	ch <- r
}

// newResponse 開始組一個回覆 req 的 Response-PDU，varbind 由呼叫端接著寫入。標頭照抄
// 請求（位元組順序也相同），只把類型換成 Response（RFC 2741 7.2.2）。
func newResponse(req header, st Status, index uint16) *encoder {
	e := newEncoder(header{
		typ:           pduResponse,
		flags:         req.flags & flagNetworkByteOrder,
		sessionID:     req.sessionID,
		transactionID: req.transactionID,
		packetID:      req.packetID,
	})
	e.u32(0) // res.sysUpTime：subagent 送的值 master 不看
	e.u16(uint16(st))
	e.u16(index)
	return e
}

// reply 回覆一個不帶 varbind 的 Response。寫入失敗會由 write 結束 Client。
func (c *Client) reply(req header, st Status, index uint16) {
	c.write(newResponse(req, st, index).bytes())
}

// serveRead 在自己的 goroutine 裡回答一個 Get、GetNext 或 GetBulk。Handler 回傳錯誤、
// panic，或值無法編碼時，回 genErr 與出錯的 search range 位置，varbind 清空
// （RFC 2741 7.2.3）。
func (c *Client) serveRead(s *Session, h header, req readRequest) {
	defer c.handlers.Done()
	e := newResponse(h, StatusNoError, 0)
	var at int // 正在處理第幾個 search range（從 1 開始），出錯時當成 res.index
	if err := c.answer(s.handler, h.typ, req, e, &at); err != nil {
		c.log.Warn("agentx: request failed", "type", h.typ, "session_id", h.sessionID, "index", at, "err", err)
		e = newResponse(h, StatusGenErr, uint16(at))
	}
	c.write(e.bytes())
}

// answer 依序處理每個 search range，把結果寫進 e。
func (c *Client) answer(hd Handler, typ pduType, req readRequest, e *encoder, at *int) (err error) {
	defer func() {
		if p := recover(); p != nil {
			c.log.Error("agentx: handler panic", "panic", p, "stack", string(debug.Stack()))
			err = fmt.Errorf("handler panic: %v", p)
		}
	}()

	ctx := c.ctx
	switch typ {
	case pduGet:
		for i, r := range req.ranges {
			*at = i + 1
			vb, err := hd.Get(ctx, r.start)
			if err != nil {
				return err
			}
			vb.Name = r.start // RFC 2741 7.2.3.1：名稱一律是請求的 OID
			if err := e.varbind(vb); err != nil {
				return err
			}
		}
	case pduGetNext:
		for i, r := range req.ranges {
			*at = i + 1
			vb, err := getNext(ctx, hd, r)
			if err != nil {
				return err
			}
			if err := e.varbind(vb); err != nil {
				return err
			}
		}
	case pduGetBulk:
		return getBulk(ctx, hd, req, e, at)
	}
	return nil
}

// getNext 呼叫 Handler.GetNext。找不到、或結果不在範圍內時，回傳名稱為起點的
// endOfMibView（RFC 2741 7.2.3.2）。
func getNext(ctx context.Context, hd Handler, r searchRange) (VarBind, error) {
	vb, err := hd.GetNext(ctx, r.start, r.include, r.end)
	if err != nil {
		return VarBind{}, err
	}
	switch vb.Type {
	case TypeEndOfMIBView, TypeNoSuchObject, TypeNoSuchInstance:
		return VarBind{Name: r.start, Type: TypeEndOfMIBView}, nil
	}
	if !r.contains(vb.Name) {
		return VarBind{Name: r.start, Type: TypeEndOfMIBView}, nil
	}
	return vb, nil
}

// getBulk 依 RFC 2741 7.2.3.3 處理 GetBulk：前 N（non_repeaters）個 search range 各做
// 一次 GetNext；其餘 R 個重複 M（max_repetitions）輪，每輪從該欄上一輪的結果往後找。
// 某一欄碰到 endOfMibView 之後，後面每一輪都重複同一個名稱的 endOfMibView；某一輪
// 全部都是 endOfMibView，或回應超過 maxPayload 時停止。
func getBulk(ctx context.Context, hd Handler, req readRequest, e *encoder, at *int) error {
	n := min(int(req.nonRepeaters), len(req.ranges))
	for i, r := range req.ranges[:n] {
		*at = i + 1
		vb, err := getNext(ctx, hd, r)
		if err != nil {
			return err
		}
		if err := e.varbind(vb); err != nil {
			return err
		}
	}

	cols := slices.Clone(req.ranges[n:]) // 每一欄下一輪的起點
	ended := make([]bool, len(cols))
	for range req.maxRepetitions {
		allEnded := true
		for j := range cols {
			*at = n + j + 1
			vb := VarBind{Name: cols[j].start, Type: TypeEndOfMIBView}
			if !ended[j] {
				var err error
				if vb, err = getNext(ctx, hd, cols[j]); err != nil {
					return err
				}
				if vb.Type == TypeEndOfMIBView {
					ended[j] = true
				} else {
					allEnded = false
					cols[j].start, cols[j].include = vb.Name, false
				}
			}
			if err := e.varbind(vb); err != nil {
				return err
			}
		}
		if allEnded || len(e.buf) > maxPayload {
			break
		}
	}
	return nil
}
