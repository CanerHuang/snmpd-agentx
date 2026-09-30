package agentx

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net/netip"
	"slices"
	"time"
)

// byteOrder 同時提供讀與寫；binary.BigEndian、binary.LittleEndian 都符合。
type byteOrder interface {
	binary.ByteOrder
	binary.AppendByteOrder
}

// maxSubIDs 是一個 OID 最多的 sub-identifier 數（RFC 2741 5.1）。
const maxSubIDs = 128

// internet 是 OID 標頭 prefix 欄位代表的前綴 1.3.6.1。
var internet = OID{1, 3, 6, 1}

// encoder 組出一個 PDU。多位元組整數的位元組順序必須和標頭的 NETWORK_BYTE_ORDER
// 一致，由 newEncoder 依標頭決定。
type encoder struct {
	buf   []byte
	order byteOrder
}

// newEncoder 先寫入標頭，payload 長度由 bytes 補上。
func newEncoder(h header) *encoder {
	e := &encoder{buf: make([]byte, 0, 128), order: h.order()}
	e.buf = append(e.buf, 1, byte(h.typ), h.flags, 0)
	e.u32(h.sessionID)
	e.u32(h.transactionID)
	e.u32(h.packetID)
	e.u32(0) // h.payload_length
	return e
}

// bytes 補上 payload 長度，回傳完整的 PDU。
func (e *encoder) bytes() []byte {
	e.order.PutUint32(e.buf[16:], uint32(len(e.buf)-headerSize))
	return e.buf
}

func (e *encoder) u8(v uint8)   { e.buf = append(e.buf, v) }
func (e *encoder) u16(v uint16) { e.buf = e.order.AppendUint16(e.buf, v) }
func (e *encoder) u32(v uint32) { e.buf = e.order.AppendUint32(e.buf, v) }
func (e *encoder) u64(v uint64) { e.buf = e.order.AppendUint64(e.buf, v) }

// oid 寫入一個 OID。1.3.6.1.x（x 為 1–255）開頭的 OID 用 prefix 欄位壓縮掉前 5 個
// sub-identifier；include 只在 search range 的起點有意義。
func (e *encoder) oid(o OID, include bool) error {
	if len(o) > maxSubIDs {
		return fmt.Errorf("OID has %d sub-identifiers, max %d", len(o), maxSubIDs)
	}
	var prefix uint8
	if len(o) > len(internet) && slices.Equal(o[:len(internet)], internet) && o[4] >= 1 && o[4] <= math.MaxUint8 {
		prefix = uint8(o[4])
		o = o[5:]
	}
	var inc uint8
	if include {
		inc = 1
	}
	e.buf = append(e.buf, uint8(len(o)), prefix, inc, 0)
	for _, v := range o {
		e.u32(v)
	}
	return nil
}

// appendOctets 寫入 octet string：4 byte 長度、內容，再補 0 到 4 byte 對齊
// （RFC 2741 5.3）。整個 PDU 從頭就是 4 byte 對齊，所以看 buf 長度即可。
func appendOctets[T string | []byte](e *encoder, s T) {
	e.u32(uint32(len(s)))
	e.buf = append(e.buf, s...)
	for len(e.buf)%4 != 0 {
		e.buf = append(e.buf, 0)
	}
}

// varbind 寫入一個 varbind。Value 與 Type 對不上時不留下任何資料，並回傳錯誤。
func (e *encoder) varbind(vb VarBind) error {
	mark := len(e.buf)
	e.u16(uint16(vb.Type))
	e.u16(0)
	err := e.oid(vb.Name, false)
	if err == nil {
		err = e.value(vb.Type, vb.Value)
	}
	if err != nil {
		e.buf = e.buf[:mark]
		return fmt.Errorf("varbind %s: %w", vb.Name, err)
	}
	return nil
}

// value 依型別寫入 v.data（RFC 2741 5.4）。
func (e *encoder) value(t VarType, v any) error {
	switch t {
	case TypeInteger:
		n, ok := toInt64(v)
		if !ok || n < math.MinInt32 || n > math.MaxInt32 {
			return badValue(t, v)
		}
		e.u32(uint32(int32(n)))
	case TypeCounter32, TypeGauge32:
		n, ok := toUint64(v)
		if !ok || n > math.MaxUint32 {
			return badValue(t, v)
		}
		e.u32(uint32(n))
	case TypeTimeTicks:
		if d, ok := v.(time.Duration); ok {
			if d < 0 {
				return badValue(t, v)
			}
			e.u32(uint32(d / (10 * time.Millisecond))) // TimeTicks 以 2^32 為模
			return nil
		}
		n, ok := toUint64(v)
		if !ok || n > math.MaxUint32 {
			return badValue(t, v)
		}
		e.u32(uint32(n))
	case TypeCounter64:
		n, ok := toUint64(v)
		if !ok {
			return badValue(t, v)
		}
		e.u64(n)
	case TypeOctetString, TypeOpaque:
		switch s := v.(type) {
		case string:
			appendOctets(e, s)
		case []byte:
			appendOctets(e, s)
		default:
			return badValue(t, v)
		}
	case TypeObjectIdentifier:
		o, ok := v.(OID)
		if !ok {
			return badValue(t, v)
		}
		return e.oid(o, false)
	case TypeIPAddress:
		ip, ok := toIPv4(v)
		if !ok {
			return badValue(t, v)
		}
		appendOctets(e, ip[:])
	case TypeNull, TypeNoSuchObject, TypeNoSuchInstance, TypeEndOfMIBView:
	default:
		return fmt.Errorf("unsupported type %s", t)
	}
	return nil
}

// errShort 表示 payload 在欄位讀完之前就結束了。
var errShort = errors.New("payload too short")

// decoder 依序讀取 payload。資料不足或格式錯誤時記下第一個錯誤，之後的讀取都回傳
// 零值；呼叫端讀完再檢查 err。
type decoder struct {
	b     []byte
	order binary.ByteOrder
	err   error
}

func newDecoder(h header, payload []byte) *decoder {
	return &decoder{b: payload, order: h.order()}
}

func (d *decoder) fail(err error) {
	if d.err == nil {
		d.err = err
	}
	d.b = nil
}

// more 表示還有資料可以讀。
func (d *decoder) more() bool { return d.err == nil && len(d.b) > 0 }

func (d *decoder) take(n int) []byte {
	if d.err != nil {
		return nil
	}
	if n > len(d.b) {
		d.fail(errShort)
		return nil
	}
	p := d.b[:n:n]
	d.b = d.b[n:]
	return p
}

func (d *decoder) u8() uint8 {
	if p := d.take(1); d.err == nil {
		return p[0]
	}
	return 0
}

func (d *decoder) u16() uint16 {
	if p := d.take(2); d.err == nil {
		return d.order.Uint16(p)
	}
	return 0
}

func (d *decoder) u32() uint32 {
	if p := d.take(4); d.err == nil {
		return d.order.Uint32(p)
	}
	return 0
}

func (d *decoder) u64() uint64 {
	if p := d.take(8); d.err == nil {
		return d.order.Uint64(p)
	}
	return 0
}

// oid 讀一個 OID 與它的 include 欄位；prefix 不為 0 時還原 1.3.6.1.<prefix> 前綴。
func (d *decoder) oid() (OID, bool) {
	h := d.take(4)
	if d.err != nil {
		return nil, false
	}
	n, prefix, include := int(h[0]), h[1], h[2] != 0
	if n > maxSubIDs {
		d.fail(fmt.Errorf("OID has %d sub-identifiers, max %d", n, maxSubIDs))
		return nil, false
	}
	raw := d.take(4 * n)
	if d.err != nil {
		return nil, false
	}
	o := make(OID, 0, 5+n)
	if prefix != 0 {
		o = append(o, internet...)
		o = append(o, uint32(prefix))
	}
	for i := range n {
		o = append(o, d.order.Uint32(raw[4*i:]))
	}
	return o, include
}

// octets 讀一個 octet string，並跳過對齊用的補齊。回傳的 slice 指向 payload。
func (d *decoder) octets() []byte {
	n := d.u32()
	if d.err != nil {
		return nil
	}
	if uint64(n) > uint64(len(d.b)) {
		d.fail(errShort)
		return nil
	}
	s := d.take(int(n))
	d.take(int(-n & 3))
	return s
}

// varbind 讀一個 varbind。值的 Go 型別見 VarBind 的說明。
func (d *decoder) varbind() VarBind {
	t := VarType(d.u16())
	d.u16()
	name, _ := d.oid()
	vb := VarBind{Name: name, Type: t}
	switch t {
	case TypeInteger:
		vb.Value = int32(d.u32())
	case TypeCounter32, TypeGauge32, TypeTimeTicks:
		vb.Value = d.u32()
	case TypeCounter64:
		vb.Value = d.u64()
	case TypeOctetString, TypeOpaque:
		vb.Value = d.octets()
	case TypeIPAddress:
		s := d.octets()
		if d.err == nil && len(s) != 4 {
			d.fail(fmt.Errorf("IpAddress has %d octets", len(s)))
		}
		if d.err == nil {
			vb.Value = netip.AddrFrom4([4]byte(s))
		}
	case TypeObjectIdentifier:
		o, _ := d.oid()
		vb.Value = o
	case TypeNull, TypeNoSuchObject, TypeNoSuchInstance, TypeEndOfMIBView:
	default:
		d.fail(fmt.Errorf("unknown varbind type %d", t))
	}
	return vb
}
