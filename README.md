# snmpd-agentx

唯讀 AgentX subagent（RFC 2741 的子集），讓 Go 程式掛在 net-snmp `snmpd` 底下提供自己的 MIB。

- PDU：Open、Close、Register、Response、Get、GetNext、GetBulk、Ping；三種查詢都支援多個 search range。
- 唯讀：TestSet 一律回 `notWritable`；不支援 Notify、IndexAllocate、AgentCaps。
- 連線：unix socket 與 tcp。讀寫錯誤一律回傳給呼叫端，不 panic，也不在內部自行重連。

## 安裝

```bash
go get github.com/CanerHuang/snmpd-agentx
```

需求：Go 1.26+，只用標準庫。

## 使用方式

實作 `Handler`，然後連線 → 開 session → 註冊 subtree → 等到 session 結束：

```go
import agentx "github.com/CanerHuang/snmpd-agentx"

type Handler interface {
	Get(ctx context.Context, oid agentx.OID) (agentx.VarBind, error)
	GetNext(ctx context.Context, from agentx.OID, include bool, to agentx.OID) (agentx.VarBind, error)
}
```

```go
c, err := agentx.Dial(ctx, "unix", "/var/agentx/master") // 或 "tcp", "localhost:705"
if err != nil {
	return err
}
defer c.Close()

s, err := c.Open(ctx, agentx.OID{1, 3, 6, 1, 4, 1, 99999}, "my-agent", handler)
if err != nil {
	return err
}
if err := s.Register(ctx, agentx.OID{1, 3, 6, 1, 4, 1, 99999, 1}, agentx.DefaultPriority); err != nil {
	return err
}

select {
case <-ctx.Done():
	return ctx.Err()
case <-s.Done():
	return s.Err() // snmpd 重啟、斷線或關閉 session；由呼叫端決定何時重連
}
```

- 找不到變數時，`Get` 回傳 `Type` 為 `TypeNoSuchObject` / `TypeNoSuchInstance` 的 `VarBind`，`GetNext` 回傳 `TypeEndOfMIBView`；`Name` 由套件補上。
- `GetNext` 的結果落在 search range 之外時，套件一律當成 `endOfMibView`，避免 master 走訪時繞圈。
- `Value` 的 Go 型別見 `VarBind` 的說明，例如 Integer 用 `int32`、Gauge32 用 `uint32`、TimeTicks 可以直接給 `time.Duration`。
- log 用 `log/slog`，預設 `slog.Default()`，可以用 `agentx.WithLogger` 指定。完整的重連範例見 `example_test.go`。

## 錯誤處理

| 情況 | 行為 |
|---|---|
| 讀寫失敗、master 斷線 | Client 關閉連線、結束所有 Session，`Err()` 回傳原因（斷線是包著 `io.EOF` 的錯誤）；不自行重連 |
| 標頭無法解析（版本不是 1、長度不是 4 的倍數、超過 1 MiB） | 無法知道下一個 PDU 從哪裡開始，同上 |
| payload 無法解析 | 回 `parseError`，連線照常 |
| 不認得的 PDU 類型 | 回 `parseError` 並記 log，連線照常 |
| subagent 不該收到的 PDU（Open、Register、Notify…） | 回 `processingError` 並記 log，連線照常 |
| 不存在的 session / 非預設 context | 回 `notOpen` / `unsupportedContext` |
| TestSet | 回 `notWritable`；CleanupSet 不回覆 |
| Handler 回傳錯誤、panic，或值與型別對不上 | 該次查詢回 `genErr` 與出錯的 search range 位置 |
| master 送 Close | 回覆後結束該 Session，`Err()` 是 `*SessionClosedError` |
| 送出的請求被拒（例如重複註冊） | 回傳 `*StatusError` |

## 測試

```bash
go test -race ./...

# 整合測試：用真的 net-snmp snmpd 當 master，需要 snmpd 與 snmpget、snmpwalk 等指令
AGENTX_SNMPD=/usr/sbin/snmpd go test -tags integration -run Integration -v .
```

## 目錄結構

單一 package `agentx`，不分子套件。

```
snmpd-agentx/
├── go.mod               module github.com/CanerHuang/snmpd-agentx
├── LICENSE              MIT
├── README.md
├── doc.go               套件說明與使用流程
├── oid.go               OID：ParseOID、String、Compare
├── varbind.go           VarType 常數、VarBind、Go 值與 SNMP 型別的對應
├── errors.go            Status（res.error）、CloseReason、StatusError、SessionClosedError、ErrClosed
├── handler.go           Handler 介面：Get(oid)、GetNext(from, include, to)
├── client.go            Client：Dial / NewClient、讀取迴圈、寫入序列化、請求與回應配對、關閉
├── session.go           Session：Open、Register、Ping、Close、Done、Err
├── serve.go             處理 master 送來的 PDU：Get、GetNext、GetBulk、Set 類、Close、不認得的 PDU
├── pdu.go               20 byte 標頭、PDU 類型與旗標、各 PDU 的解析
├── codec.go             編解碼：位元組順序、OID 前綴壓縮、octet string 補齊、varbind
├── codec_test.go        編碼對照 RFC 2741 範例、各型別往返、異常資料
├── client_test.go       假 master：多 OID Get、GetBulk、封包分段、連續兩個 PDU、異常封包、不支援的 PDU、斷線
├── example_test.go      使用範例
└── integration_test.go  build tag `integration`：跑真的 snmpd，用 snmpget / snmpwalk / snmpbulkwalk 驗證
```

## License

MIT
