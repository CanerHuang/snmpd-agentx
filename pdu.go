package agentx

import (
	"encoding/binary"
	"fmt"
	"strconv"
)

// pduType 是標頭的 h.type（RFC 2741 6.1）。
type pduType uint8

const (
	pduOpen            pduType = 1
	pduClose           pduType = 2
	pduRegister        pduType = 3
	pduUnregister      pduType = 4
	pduGet             pduType = 5
	pduGetNext         pduType = 6
	pduGetBulk         pduType = 7
	pduTestSet         pduType = 8
	pduCommitSet       pduType = 9
	pduUndoSet         pduType = 10
	pduCleanupSet      pduType = 11
	pduNotify          pduType = 12
	pduPing            pduType = 13
	pduIndexAllocate   pduType = 14
	pduIndexDeallocate pduType = 15
	pduAddAgentCaps    pduType = 16
	pduRemoveAgentCaps pduType = 17
	pduResponse        pduType = 18
)

var pduNames = [...]string{
	pduOpen:            "Open",
	pduClose:           "Close",
	pduRegister:        "Register",
	pduUnregister:      "Unregister",
	pduGet:             "Get",
	pduGetNext:         "GetNext",
	pduGetBulk:         "GetBulk",
	pduTestSet:         "TestSet",
	pduCommitSet:       "CommitSet",
	pduUndoSet:         "UndoSet",
	pduCleanupSet:      "CleanupSet",
	pduNotify:          "Notify",
	pduPing:            "Ping",
	pduIndexAllocate:   "IndexAllocate",
	pduIndexDeallocate: "IndexDeallocate",
	pduAddAgentCaps:    "AddAgentCaps",
	pduRemoveAgentCaps: "RemoveAgentCaps",
	pduResponse:        "Response",
}

func (t pduType) String() string {
	if int(t) < len(pduNames) && pduNames[t] != "" {
		return pduNames[t]
	}
	return "PDU(" + strconv.Itoa(int(t)) + ")"
}

// 標頭 h.flags 用到的位元。
const (
	flagNonDefaultContext uint8 = 1 << 3 // payload 開頭帶 context
	flagNetworkByteOrder  uint8 = 1 << 4 // 多位元組整數用 big endian
)

const (
	headerSize = 20

	// maxPayload 是接受的 payload 上限，超過就視為連線錯誤；GetBulk 的回應長到這個
	// 大小也會停止重複。
	maxPayload = 1 << 20
)

// header 是 20 byte 的 PDU 標頭。h.version 固定為 1，不另外存。
type header struct {
	typ           pduType
	flags         uint8
	sessionID     uint32
	transactionID uint32
	packetID      uint32
	payloadLen    uint32
}

// order 依 NETWORK_BYTE_ORDER 決定整個 PDU（包含標頭後段）的位元組順序。
func (h header) order() byteOrder {
	if h.flags&flagNetworkByteOrder != 0 {
		return binary.BigEndian
	}
	return binary.LittleEndian
}

// parseHeader 解析標頭。回傳錯誤表示位元組流已經無法信任（不知道下一個 PDU 從哪裡
// 開始），呼叫端必須結束連線。
func parseHeader(b []byte) (header, error) {
	if b[0] != 1 {
		return header{}, fmt.Errorf("unsupported version %d", b[0])
	}
	h := header{typ: pduType(b[1]), flags: b[2]}
	o := h.order()
	h.sessionID = o.Uint32(b[4:])
	h.transactionID = o.Uint32(b[8:])
	h.packetID = o.Uint32(b[12:])
	h.payloadLen = o.Uint32(b[16:])
	if h.payloadLen%4 != 0 {
		return header{}, fmt.Errorf("payload length %d is not a multiple of 4", h.payloadLen)
	}
	if h.payloadLen > maxPayload {
		return header{}, fmt.Errorf("payload length %d exceeds %d", h.payloadLen, maxPayload)
	}
	return h, nil
}

// searchRange 是查詢的範圍：從 start 開始（include 決定含不含 start），到 end 之前
// 為止；end 為空表示沒有上界（RFC 2741 5.2）。
type searchRange struct {
	start   OID
	include bool
	end     OID
}

// contains 判斷 o 是否落在範圍內。
func (r searchRange) contains(o OID) bool {
	if c := o.Compare(r.start); c < 0 || c == 0 && !r.include {
		return false
	}
	return len(r.end) == 0 || o.Compare(r.end) < 0
}

// readRequest 是解析後的 Get、GetNext、GetBulk。
type readRequest struct {
	nonRepeaters   uint16 // 只有 GetBulk 用
	maxRepetitions uint16 // 只有 GetBulk 用
	ranges         []searchRange
}

// response 是解析後的 Response-PDU。
type response struct {
	sessionID uint32
	status    Status
	index     uint16
	varbinds  []VarBind
}

// skipContext 在設了 NON_DEFAULT_CONTEXT 時跳過 payload 開頭的 context，回傳有沒有
// context（RFC 2741 6.1.1）。
func skipContext(h header, d *decoder) bool {
	if h.flags&flagNonDefaultContext == 0 {
		return false
	}
	d.octets()
	return true
}

// parseRead 解析 Get、GetNext、GetBulk 的 payload。
func parseRead(h header, payload []byte) (req readRequest, hasContext bool, err error) {
	d := newDecoder(h, payload)
	hasContext = skipContext(h, d)
	if h.typ == pduGetBulk {
		req.nonRepeaters = d.u16()
		req.maxRepetitions = d.u16()
	}
	for d.more() {
		start, include := d.oid()
		end, _ := d.oid()
		req.ranges = append(req.ranges, searchRange{start: start, include: include, end: end})
	}
	return req, hasContext, d.err
}

// parseTestSet 解析 TestSet 的 payload。
func parseTestSet(h header, payload []byte) (vbs []VarBind, hasContext bool, err error) {
	d := newDecoder(h, payload)
	hasContext = skipContext(h, d)
	for d.more() {
		vbs = append(vbs, d.varbind())
	}
	return vbs, hasContext, d.err
}

// parsePing 解析 Ping 的 payload（只有 context）。
func parsePing(h header, payload []byte) (hasContext bool, err error) {
	d := newDecoder(h, payload)
	hasContext = skipContext(h, d)
	return hasContext, d.err
}

// parseClose 解析 Close 的 payload。
func parseClose(h header, payload []byte) (CloseReason, error) {
	d := newDecoder(h, payload)
	reason := CloseReason(d.u8())
	d.take(3)
	return reason, d.err
}

// parseResponse 解析 Response 的 payload。
func parseResponse(h header, payload []byte) (*response, error) {
	d := newDecoder(h, payload)
	r := &response{sessionID: h.sessionID}
	d.u32() // res.sysUpTime
	r.status = Status(d.u16())
	r.index = d.u16()
	for d.more() {
		r.varbinds = append(r.varbinds, d.varbind())
	}
	if d.err != nil {
		return nil, d.err
	}
	return r, nil
}
