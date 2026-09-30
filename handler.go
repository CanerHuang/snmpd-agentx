package agentx

import "context"

// Handler 回答 master 轉來的查詢，多個 goroutine 可能同時呼叫。ctx 在 Client 關閉時
// 取消。
//
// 回傳 error 時，整個查詢回 genErr 給 master（RFC 2741 7.2.3）；找不到變數不是錯誤，
// 請用下面說明的例外型別表示。Handler panic 也會被攔下，當成回傳 error。
type Handler interface {
	// Get 回傳名稱剛好是 oid 的變數。找不到時回傳 Type 為 TypeNoSuchObject（oid 不在
	// 任何物件底下）或 TypeNoSuchInstance（物件存在但沒有這個 instance）的 VarBind。
	// 回傳的 Name 不重要，一律換成 oid。
	Get(ctx context.Context, oid OID) (VarBind, error)

	// GetNext 回傳字典順序在 from 之後的第一個變數（include 為 true 時可以等於 from），
	// 而且名稱必須在 to 之前；to 為空表示沒有上界。找不到時回傳 Type 為
	// TypeEndOfMIBView 的 VarBind。結果落在範圍外也會被當成 TypeEndOfMIBView，避免
	// master 走訪時繞圈。
	GetNext(ctx context.Context, from OID, include bool, to OID) (VarBind, error)
}
