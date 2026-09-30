package agentx

import (
	"bytes"
	"encoding/binary"
	"net"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"
)

// be 把 uint32 串成 big endian 的 byte，方便寫預期的編碼。
func be(vs ...uint32) []byte {
	var b []byte
	for _, v := range vs {
		b = binary.BigEndian.AppendUint32(b, v)
	}
	return b
}

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// 對照 RFC 2741 5.1、5.2 的範例與前綴壓縮的邊界。
func TestEncodeOID(t *testing.T) {
	tests := []struct {
		name    string
		oid     OID
		include bool
		want    []byte
	}{
		{"sysDescr.0 (RFC 5.1)", OID{1, 3, 6, 1, 2, 1, 1, 1, 0}, false, cat([]byte{4, 2, 0, 0}, be(1, 1, 1, 0))},
		{"1.2.3.4 (RFC 5.1)", OID{1, 2, 3, 4}, false, cat([]byte{4, 0, 0, 0}, be(1, 2, 3, 4))},
		{"search range start (RFC 5.2)", OID{1, 3, 6, 1, 2, 1, 25, 2}, true, cat([]byte{3, 2, 1, 0}, be(1, 25, 2))},
		{"search range end (RFC 5.2)", OID{1, 3, 6, 1, 2, 1, 25, 2, 1}, false, cat([]byte{4, 2, 0, 0}, be(1, 25, 2, 1))},
		{"null", OID{}, false, []byte{0, 0, 0, 0}},
		{"exactly internet.x", OID{1, 3, 6, 1, 4}, false, []byte{0, 4, 0, 0}},
		{"internet itself is not compressed", OID{1, 3, 6, 1}, false, cat([]byte{4, 0, 0, 0}, be(1, 3, 6, 1))},
		{"x = 0 is not compressed", OID{1, 3, 6, 1, 0, 5}, false, cat([]byte{6, 0, 0, 0}, be(1, 3, 6, 1, 0, 5))},
		{"x > 255 is not compressed", OID{1, 3, 6, 1, 256, 5}, false, cat([]byte{6, 0, 0, 0}, be(1, 3, 6, 1, 256, 5))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &encoder{order: binary.BigEndian}
			if err := e.oid(tt.oid, tt.include); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(e.buf, tt.want) {
				t.Fatalf("got  % x\nwant % x", e.buf, tt.want)
			}
			d := &decoder{b: e.buf, order: binary.BigEndian}
			got, include := d.oid()
			if d.err != nil || got.Compare(tt.oid) != 0 || include != tt.include || len(d.b) != 0 {
				t.Fatalf("decode = %v %v %v, rest %d", got, include, d.err, len(d.b))
			}
		})
	}
}

func TestEncodeOIDLittleEndian(t *testing.T) {
	e := &encoder{order: binary.LittleEndian}
	if err := e.oid(OID{1, 2, 0x01020304}, false); err != nil {
		t.Fatal(err)
	}
	want := []byte{3, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 4, 3, 2, 1}
	if !bytes.Equal(e.buf, want) {
		t.Fatalf("got % x, want % x", e.buf, want)
	}
}

func TestEncodeOIDTooLong(t *testing.T) {
	e := &encoder{order: binary.BigEndian}
	if err := e.oid(make(OID, maxSubIDs+1), false); err == nil {
		t.Fatal("expected error for 129 sub-identifiers")
	}
	if err := e.oid(make(OID, maxSubIDs), false); err != nil {
		t.Fatalf("128 sub-identifiers: %v", err)
	}
}

func TestOctetStringPadding(t *testing.T) {
	tests := []struct {
		in   string
		want []byte
	}{
		{"", be(0)},
		{"a", cat(be(1), []byte{'a', 0, 0, 0})},
		{"abc", cat(be(3), []byte{'a', 'b', 'c', 0})},
		{"abcd", cat(be(4), []byte("abcd"))},
		{"abcde", cat(be(5), []byte{'a', 'b', 'c', 'd', 'e', 0, 0, 0})},
	}
	for _, tt := range tests {
		e := &encoder{order: binary.BigEndian}
		appendOctets(e, tt.in)
		if !bytes.Equal(e.buf, tt.want) {
			t.Errorf("%q: got % x, want % x", tt.in, e.buf, tt.want)
		}
		d := &decoder{b: e.buf, order: binary.BigEndian}
		if got := d.octets(); d.err != nil || string(got) != tt.in || len(d.b) != 0 {
			t.Errorf("%q: decode = %q %v, rest %d", tt.in, got, d.err, len(d.b))
		}
	}
}

func TestEncodeHeader(t *testing.T) {
	h := header{typ: pduGet, flags: flagNetworkByteOrder, sessionID: 0x01020304, transactionID: 5, packetID: 6}
	e := newEncoder(h)
	e.u32(7)
	got := e.bytes()
	want := cat([]byte{1, 5, 0x10, 0}, be(0x01020304, 5, 6, 4, 7))
	if !bytes.Equal(got, want) {
		t.Fatalf("big endian: got % x, want % x", got, want)
	}

	h.flags = 0
	got = newEncoder(h).bytes()
	want = []byte{1, 5, 0, 0, 4, 3, 2, 1, 5, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0}
	if !bytes.Equal(got, want) {
		t.Fatalf("little endian: got % x, want % x", got, want)
	}
	back, err := parseHeader(got)
	if err != nil || back.sessionID != 0x01020304 || back.packetID != 6 || back.typ != pduGet {
		t.Fatalf("parseHeader = %+v, %v", back, err)
	}
}

func TestParseHeaderErrors(t *testing.T) {
	ok := newEncoder(header{typ: pduGet, flags: flagNetworkByteOrder}).bytes()
	tests := map[string]func(b []byte){
		"version":      func(b []byte) { b[0] = 2 },
		"not aligned":  func(b []byte) { binary.BigEndian.PutUint32(b[16:], 6) },
		"too long":     func(b []byte) { binary.BigEndian.PutUint32(b[16:], maxPayload+4) },
		"huge (4 GiB)": func(b []byte) { binary.BigEndian.PutUint32(b[16:], 0xfffffffc) },
	}
	for name, corrupt := range tests {
		b := bytes.Clone(ok)
		corrupt(b)
		if _, err := parseHeader(b); err == nil {
			t.Errorf("%s: expected error", name)
		}
	}
}

// 每種型別編碼後再解回來，兩種位元組順序都測。
func TestVarBindRoundTrip(t *testing.T) {
	name := OID{1, 3, 6, 1, 4, 1, 99999, 1, 0}
	tests := []struct {
		typ  VarType
		in   any
		want any
	}{
		{TypeInteger, int32(-5), int32(-5)},
		{TypeInteger, 7, int32(7)},
		{TypeInteger, int64(-2147483648), int32(-2147483648)},
		{TypeInteger, uint8(200), int32(200)},
		{TypeOctetString, "ACME", []byte("ACME")},
		{TypeOctetString, []byte{0, 1, 2}, []byte{0, 1, 2}},
		{TypeOctetString, "", []byte{}},
		{TypeOpaque, []byte{9, 8, 7, 6, 5}, []byte{9, 8, 7, 6, 5}},
		{TypeObjectIdentifier, OID{1, 3, 6, 1, 6, 3, 1}, OID{1, 3, 6, 1, 6, 3, 1}},
		{TypeIPAddress, netip.MustParseAddr("192.0.2.1"), netip.MustParseAddr("192.0.2.1")},
		{TypeIPAddress, netip.MustParseAddr("::ffff:192.0.2.2"), netip.MustParseAddr("192.0.2.2")},
		{TypeIPAddress, net.IPv4(192, 0, 2, 3), netip.MustParseAddr("192.0.2.3")},
		{TypeIPAddress, [4]byte{10, 0, 0, 1}, netip.MustParseAddr("10.0.0.1")},
		{TypeCounter32, uint32(0xffffffff), uint32(0xffffffff)},
		{TypeGauge32, uint(5), uint32(5)},
		{TypeGauge32, 12, uint32(12)},
		{TypeTimeTicks, 90 * time.Second, uint32(9000)},
		{TypeTimeTicks, 15 * time.Millisecond, uint32(1)},
		{TypeTimeTicks, uint32(42), uint32(42)},
		{TypeCounter64, uint64(1 << 40), uint64(1 << 40)},
		{TypeCounter64, uint64(0xffffffffffffffff), uint64(0xffffffffffffffff)},
		{TypeCounter64, 3, uint64(3)},
		{TypeNull, nil, nil},
		{TypeNoSuchObject, nil, nil},
		{TypeNoSuchInstance, nil, nil},
		{TypeEndOfMIBView, "ignored", nil},
	}
	for _, order := range []byteOrder{binary.BigEndian, binary.LittleEndian} {
		for _, tt := range tests {
			e := &encoder{order: order}
			if err := e.varbind(VarBind{Name: name, Type: tt.typ, Value: tt.in}); err != nil {
				t.Errorf("%v %s %#v: %v", order, tt.typ, tt.in, err)
				continue
			}
			if len(e.buf)%4 != 0 {
				t.Errorf("%v %s: encoding length %d not aligned", order, tt.typ, len(e.buf))
			}
			d := &decoder{b: e.buf, order: order}
			got := d.varbind()
			if d.err != nil || len(d.b) != 0 {
				t.Errorf("%v %s: decode error %v, rest %d", order, tt.typ, d.err, len(d.b))
				continue
			}
			if got.Type != tt.typ || got.Name.Compare(name) != 0 || !reflect.DeepEqual(got.Value, tt.want) {
				t.Errorf("%v %s %#v: decoded %v %#v, want %#v", order, tt.typ, tt.in, got.Type, got.Value, tt.want)
			}
		}
	}
}

// Counter64 在兩種位元組順序下都是完整的 8 byte 整數。
func TestCounter64ByteOrder(t *testing.T) {
	for _, tc := range []struct {
		order byteOrder
		want  []byte
	}{
		{binary.BigEndian, []byte{1, 2, 3, 4, 5, 6, 7, 8}},
		{binary.LittleEndian, []byte{8, 7, 6, 5, 4, 3, 2, 1}},
	} {
		e := &encoder{order: tc.order}
		if err := e.value(TypeCounter64, uint64(0x0102030405060708)); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(e.buf, tc.want) {
			t.Errorf("%v: got % x, want % x", tc.order, e.buf, tc.want)
		}
	}
}

func TestVarBindBadValue(t *testing.T) {
	tests := []struct {
		typ VarType
		v   any
	}{
		{TypeInteger, "42"},
		{TypeInteger, int64(1 << 40)},
		{TypeInteger, uint64(1 << 63)},
		{TypeInteger, nil},
		{TypeCounter32, -1},
		{TypeGauge32, uint64(1 << 32)},
		{TypeTimeTicks, -time.Second},
		{TypeTimeTicks, "1s"},
		{TypeCounter64, -1},
		{TypeCounter64, 1.5},
		{TypeOctetString, 5},
		{TypeOpaque, nil},
		{TypeObjectIdentifier, "1.3.6"},
		{TypeObjectIdentifier, make(OID, maxSubIDs+1)},
		{TypeIPAddress, netip.MustParseAddr("2001:db8::1")},
		{TypeIPAddress, net.ParseIP("2001:db8::1")},
		{TypeIPAddress, "192.0.2.1"},
		{0, nil},
		{VarType(99), nil},
	}
	for _, tt := range tests {
		e := &encoder{order: binary.BigEndian, buf: []byte{0xaa, 0xbb, 0xcc, 0xdd}}
		if err := e.varbind(VarBind{Name: OID{1, 3}, Type: tt.typ, Value: tt.v}); err == nil {
			t.Errorf("%s %#v: expected error", tt.typ, tt.v)
		}
		if len(e.buf) != 4 {
			t.Errorf("%s %#v: failed varbind left %d bytes behind", tt.typ, tt.v, len(e.buf)-4)
		}
	}
}

func TestDecodeMalformed(t *testing.T) {
	tests := map[string][]byte{
		"OID header cut short":         {4, 0},
		"too many sub-identifiers":     cat([]byte{129, 0, 0, 0}, make([]byte, 129*4)),
		"sub-identifiers missing":      cat([]byte{3, 0, 0, 0}, be(1, 2)),
		"octet string longer than pdu": cat(be(6), []byte("abcd")),
		"unknown varbind type":         cat([]byte{0, 99, 0, 0}, []byte{1, 0, 0, 0}, be(1)),
		"varbind value missing":        cat([]byte{0, 2, 0, 0}, []byte{1, 0, 0, 0}, be(1)),
		"IpAddress of 5 octets":        cat([]byte{0, 64, 0, 0}, []byte{0, 0, 0, 0}, be(5), []byte{1, 2, 3, 4, 5, 0, 0, 0}),
	}
	for name, b := range tests {
		d := &decoder{b: b, order: binary.BigEndian}
		if strings.HasPrefix(name, "OID") || strings.HasPrefix(name, "too many") || strings.HasPrefix(name, "sub-") {
			d.oid()
		} else if strings.HasPrefix(name, "octet") {
			d.octets()
		} else {
			d.varbind()
		}
		if d.err == nil {
			t.Errorf("%s: expected error", name)
		}
		if d.more() {
			t.Errorf("%s: decoder still reports more data after an error", name)
		}
	}
}

func TestParseReadMalformed(t *testing.T) {
	h := header{typ: pduGet, flags: flagNetworkByteOrder}
	// 只有起點、沒有終點的 search range。
	payload := cat([]byte{2, 0, 0, 0}, be(1, 2))
	if _, _, err := parseRead(h, payload); err == nil {
		t.Fatal("expected error for a search range without an end OID")
	}
	// GetBulk 缺 max_repetitions 以外的欄位時也要報錯。
	h.typ = pduGetBulk
	if _, _, err := parseRead(h, []byte{0, 1}); err == nil {
		t.Fatal("expected error for a truncated GetBulk")
	}
}

func TestSearchRangeContains(t *testing.T) {
	start, end := OID{1, 3, 6, 1, 2}, OID{1, 3, 6, 1, 3}
	tests := []struct {
		r    searchRange
		o    OID
		want bool
	}{
		{searchRange{start, false, end}, OID{1, 3, 6, 1, 2}, false},
		{searchRange{start, true, end}, OID{1, 3, 6, 1, 2}, true},
		{searchRange{start, false, end}, OID{1, 3, 6, 1, 2, 1}, true},
		{searchRange{start, false, end}, OID{1, 3, 6, 1, 3}, false},
		{searchRange{start, false, end}, OID{1, 3, 6, 1, 1, 9}, false},
		{searchRange{start, false, nil}, OID{1, 3, 6, 1, 9}, true},
		{searchRange{start, false, nil}, nil, false},
	}
	for _, tt := range tests {
		if got := tt.r.contains(tt.o); got != tt.want {
			t.Errorf("%+v contains %s = %v, want %v", tt.r, tt.o, got, tt.want)
		}
	}
}

func TestParseOID(t *testing.T) {
	for in, want := range map[string]OID{
		"1.3.6.1.2.1": {1, 3, 6, 1, 2, 1},
		".1.3.6":      {1, 3, 6},
		"":            {},
		"4294967295":  {4294967295},
	} {
		got, err := ParseOID(in)
		if err != nil || got.Compare(want) != 0 {
			t.Errorf("ParseOID(%q) = %v, %v", in, got, err)
		}
		if in != "" && strings.TrimPrefix(in, ".") != got.String() {
			t.Errorf("String() = %q", got.String())
		}
	}
	for _, in := range []string{"1..2", "1.x", "1.4294967296", "1.2."} {
		if _, err := ParseOID(in); err == nil {
			t.Errorf("ParseOID(%q): expected error", in)
		}
	}
	if (OID{1, 3, 6}).Compare(OID{1, 3, 6, 1}) != -1 || (OID{1, 4}).Compare(OID{1, 3, 6}) != 1 {
		t.Error("Compare order is wrong")
	}
}
