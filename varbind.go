package agentx

import (
	"fmt"
	"math"
	"net"
	"net/netip"
	"strconv"
)

// VarType 是 varbind 的型別（RFC 2741 5.4）。
type VarType uint16

const (
	TypeInteger          VarType = 2
	TypeOctetString      VarType = 4
	TypeNull             VarType = 5
	TypeObjectIdentifier VarType = 6
	TypeIPAddress        VarType = 64
	TypeCounter32        VarType = 65
	TypeGauge32          VarType = 66
	TypeTimeTicks        VarType = 67
	TypeOpaque           VarType = 68
	TypeCounter64        VarType = 70
	TypeNoSuchObject     VarType = 128
	TypeNoSuchInstance   VarType = 129
	TypeEndOfMIBView     VarType = 130
)

var varTypeNames = map[VarType]string{
	TypeInteger:          "Integer",
	TypeOctetString:      "OctetString",
	TypeNull:             "Null",
	TypeObjectIdentifier: "ObjectIdentifier",
	TypeIPAddress:        "IpAddress",
	TypeCounter32:        "Counter32",
	TypeGauge32:          "Gauge32",
	TypeTimeTicks:        "TimeTicks",
	TypeOpaque:           "Opaque",
	TypeCounter64:        "Counter64",
	TypeNoSuchObject:     "noSuchObject",
	TypeNoSuchInstance:   "noSuchInstance",
	TypeEndOfMIBView:     "endOfMibView",
}

func (t VarType) String() string {
	if s, ok := varTypeNames[t]; ok {
		return s
	}
	return "VarType(" + strconv.Itoa(int(t)) + ")"
}

// VarBind 是一個變數綁定：名稱、型別與值。
//
// Value 的 Go 型別必須和 Type 對得上：
//
//	TypeInteger               int32；其他整數型別也可以，但必須在 int32 範圍內
//	TypeCounter32、TypeGauge32 uint32；其他整數型別也可以，但必須在 0 到 2^32-1 之間
//	TypeTimeTicks             time.Duration（換算成百分之一秒，超過 2^32 取餘數），或同 Counter32
//	TypeCounter64             uint64；其他非負的整數型別也可以
//	TypeOctetString、TypeOpaque string 或 []byte
//	TypeObjectIdentifier      OID
//	TypeIPAddress             netip.Addr、net.IP 或 [4]byte，必須是 IPv4
//	TypeNull 與三種例外型別      不看 Value
//
// 對不上時，該次查詢回 genErr。
type VarBind struct {
	Name  OID
	Type  VarType
	Value any
}

// toInt64 把任何整數型別轉成 int64；超出 int64 的 uint64 視為失敗。
func toInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case uint:
		return int64(n), uint64(n) <= math.MaxInt64
	case uint8:
		return int64(n), true
	case uint16:
		return int64(n), true
	case uint32:
		return int64(n), true
	case uint64:
		return int64(n), n <= math.MaxInt64
	}
	return 0, false
}

// toUint64 把任何非負的整數轉成 uint64。
func toUint64(v any) (uint64, bool) {
	switch n := v.(type) {
	case uint:
		return uint64(n), true
	case uint8:
		return uint64(n), true
	case uint16:
		return uint64(n), true
	case uint32:
		return uint64(n), true
	case uint64:
		return n, true
	}
	n, ok := toInt64(v)
	return uint64(n), ok && n >= 0
}

// toIPv4 取出 IPv4 位址的 4 個 byte。
func toIPv4(v any) ([4]byte, bool) {
	switch a := v.(type) {
	case netip.Addr:
		if a = a.Unmap(); a.Is4() {
			return a.As4(), true
		}
	case net.IP:
		if ip4 := a.To4(); ip4 != nil {
			return [4]byte(ip4), true
		}
	case [4]byte:
		return a, true
	}
	return [4]byte{}, false
}

func badValue(t VarType, v any) error {
	return fmt.Errorf("value %v (%T) does not fit %s", v, v, t)
}
