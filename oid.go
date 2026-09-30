package agentx

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// OID 是 SNMP 的物件識別碼，一個元素是一個 sub-identifier。長度為 0 的 OID 就是
// null OID。
type OID []uint32

// ParseOID 解析點分十進位的 OID，例如 "1.3.6.1.2.1"；開頭的點可有可無，空字串是
// null OID。
func ParseOID(s string) (OID, error) {
	s = strings.TrimPrefix(s, ".")
	if s == "" {
		return OID{}, nil
	}
	parts := strings.Split(s, ".")
	o := make(OID, len(parts))
	for i, p := range parts {
		v, err := strconv.ParseUint(p, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("agentx: invalid OID %q", s)
		}
		o[i] = uint32(v)
	}
	return o, nil
}

// String 回傳點分十進位的表示，例如 "1.3.6.1.2.1"。
func (o OID) String() string {
	b := make([]byte, 0, len(o)*4)
	for i, v := range o {
		if i > 0 {
			b = append(b, '.')
		}
		b = strconv.AppendUint(b, uint64(v), 10)
	}
	return string(b)
}

// Compare 依字典順序比較 o 與 p：o 在前回傳 -1，相同回傳 0，在後回傳 1。前綴排在
// 較長的 OID 前面，例如 1.3.6 < 1.3.6.1。
func (o OID) Compare(p OID) int { return slices.Compare(o, p) }
