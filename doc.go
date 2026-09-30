// Package agentx 實作唯讀的 AgentX subagent（RFC 2741 的子集），讓 Go 程式掛在
// net-snmp snmpd 底下提供自己的 MIB。
//
// 使用流程：連線 → 開 session → 註冊 subtree → 等到 session 結束。
//
//	c, err := agentx.Dial(ctx, "unix", "/var/agentx/master")
//	if err != nil {
//		return err
//	}
//	defer c.Close()
//
//	s, err := c.Open(ctx, agentx.OID{1, 3, 6, 1, 4, 1, 99999}, "my-agent", handler)
//	if err != nil {
//		return err
//	}
//	if err := s.Register(ctx, agentx.OID{1, 3, 6, 1, 4, 1, 99999, 1}, agentx.DefaultPriority); err != nil {
//		return err
//	}
//
//	select {
//	case <-ctx.Done():
//		return ctx.Err()
//	case <-s.Done():
//		return s.Err() // snmpd 關閉 session 或斷線，由呼叫端決定何時重連
//	}
//
// master 送來的 Get、GetNext、GetBulk 會在各自的 goroutine 裡呼叫 Handler。每個
// search range 都會處理，回應的 varbind 與請求一一對應。
//
// 套件不會 panic，也不會自行重連：讀寫失敗或收到無法解析的標頭時，Client 關閉連線、
// 結束所有 Session，原因由 Client.Err 與 Session.Err 回傳。單一 PDU 的內容有問題
// （無法解析、不支援的類型、Handler 出錯）只回覆錯誤給 master，連線照常運作。
package agentx
