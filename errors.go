package agentx

import (
	"errors"
	"fmt"
	"strconv"
)

// Status 是 Response-PDU 的 res.error：0–18 是 SNMP 的 error-status，256–268 是
// AgentX 定義的錯誤（RFC 2741 6.2.16）。
type Status uint16

const (
	StatusNoError             Status = 0
	StatusTooBig              Status = 1
	StatusNoSuchName          Status = 2
	StatusBadValue            Status = 3
	StatusReadOnly            Status = 4
	StatusGenErr              Status = 5
	StatusNoAccess            Status = 6
	StatusWrongType           Status = 7
	StatusWrongLength         Status = 8
	StatusWrongEncoding       Status = 9
	StatusWrongValue          Status = 10
	StatusNoCreation          Status = 11
	StatusInconsistentValue   Status = 12
	StatusResourceUnavailable Status = 13
	StatusCommitFailed        Status = 14
	StatusUndoFailed          Status = 15
	StatusAuthorizationError  Status = 16
	StatusNotWritable         Status = 17
	StatusInconsistentName    Status = 18

	StatusOpenFailed            Status = 256
	StatusNotOpen               Status = 257
	StatusIndexWrongType        Status = 258
	StatusIndexAlreadyAllocated Status = 259
	StatusIndexNoneAvailable    Status = 260
	StatusIndexNotAllocated     Status = 261
	StatusUnsupportedContext    Status = 262
	StatusDuplicateRegistration Status = 263
	StatusUnknownRegistration   Status = 264
	StatusUnknownAgentCaps      Status = 265
	StatusParseError            Status = 266
	StatusRequestDenied         Status = 267
	StatusProcessingError       Status = 268
)

var statusNames = map[Status]string{
	StatusNoError:               "noError",
	StatusTooBig:                "tooBig",
	StatusNoSuchName:            "noSuchName",
	StatusBadValue:              "badValue",
	StatusReadOnly:              "readOnly",
	StatusGenErr:                "genErr",
	StatusNoAccess:              "noAccess",
	StatusWrongType:             "wrongType",
	StatusWrongLength:           "wrongLength",
	StatusWrongEncoding:         "wrongEncoding",
	StatusWrongValue:            "wrongValue",
	StatusNoCreation:            "noCreation",
	StatusInconsistentValue:     "inconsistentValue",
	StatusResourceUnavailable:   "resourceUnavailable",
	StatusCommitFailed:          "commitFailed",
	StatusUndoFailed:            "undoFailed",
	StatusAuthorizationError:    "authorizationError",
	StatusNotWritable:           "notWritable",
	StatusInconsistentName:      "inconsistentName",
	StatusOpenFailed:            "openFailed",
	StatusNotOpen:               "notOpen",
	StatusIndexWrongType:        "indexWrongType",
	StatusIndexAlreadyAllocated: "indexAlreadyAllocated",
	StatusIndexNoneAvailable:    "indexNoneAvailable",
	StatusIndexNotAllocated:     "indexNotAllocated",
	StatusUnsupportedContext:    "unsupportedContext",
	StatusDuplicateRegistration: "duplicateRegistration",
	StatusUnknownRegistration:   "unknownRegistration",
	StatusUnknownAgentCaps:      "unknownAgentCaps",
	StatusParseError:            "parseError",
	StatusRequestDenied:         "requestDenied",
	StatusProcessingError:       "processingError",
}

func (s Status) String() string {
	if name, ok := statusNames[s]; ok {
		return name
	}
	return "Status(" + strconv.Itoa(int(s)) + ")"
}

// CloseReason 是 Close-PDU 的 c.reason（RFC 2741 6.2.2）。
type CloseReason uint8

const (
	ReasonOther         CloseReason = 1
	ReasonParseError    CloseReason = 2
	ReasonProtocolError CloseReason = 3
	ReasonTimeouts      CloseReason = 4
	ReasonShutdown      CloseReason = 5
	ReasonByManager     CloseReason = 6
)

var reasonNames = map[CloseReason]string{
	ReasonOther:         "other",
	ReasonParseError:    "parseError",
	ReasonProtocolError: "protocolError",
	ReasonTimeouts:      "timeouts",
	ReasonShutdown:      "shutdown",
	ReasonByManager:     "byManager",
}

func (r CloseReason) String() string {
	if name, ok := reasonNames[r]; ok {
		return name
	}
	return "CloseReason(" + strconv.Itoa(int(r)) + ")"
}

// ErrClosed 表示 Client 或 Session 已在本地關閉。
var ErrClosed = errors.New("agentx: closed")

// StatusError 表示 master 對我們送出的請求回了非零的 res.error，例如註冊重複的
// subtree 會得到 StatusDuplicateRegistration。
type StatusError struct {
	Op     string // "open"、"register"、"ping"、"close"
	Status Status
	Index  uint16
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("agentx: %s: master returned %s", e.Op, e.Status)
}

// SessionClosedError 是 master 送 Close-PDU 結束 session 之後，Session.Err 回傳的
// 錯誤。
type SessionClosedError struct {
	Reason CloseReason
}

func (e *SessionClosedError) Error() string {
	return "agentx: session closed by master: " + e.Reason.String()
}
