package agentx_test

import (
	"context"
	"log"
	"time"

	agentx "github.com/CanerHuang/snmpd-agentx"
)

var uptimeOID = agentx.OID{1, 3, 6, 1, 4, 1, 99999, 1, 1, 0}

// uptime 只提供一個變數：程式啟動到現在的時間。
type uptime struct{ start time.Time }

func (u uptime) Get(_ context.Context, oid agentx.OID) (agentx.VarBind, error) {
	if oid.Compare(uptimeOID) == 0 {
		return agentx.VarBind{Type: agentx.TypeTimeTicks, Value: time.Since(u.start)}, nil
	}
	return agentx.VarBind{Type: agentx.TypeNoSuchObject}, nil
}

func (u uptime) GetNext(_ context.Context, from agentx.OID, include bool, to agentx.OID) (agentx.VarBind, error) {
	c := uptimeOID.Compare(from)
	if (c > 0 || c == 0 && include) && (len(to) == 0 || uptimeOID.Compare(to) < 0) {
		return agentx.VarBind{Name: uptimeOID, Type: agentx.TypeTimeTicks, Value: time.Since(u.start)}, nil
	}
	return agentx.VarBind{Type: agentx.TypeEndOfMIBView}, nil
}

// serve 跑一輪「連線 → 開 session → 註冊 → 等到結束」。
func serve(ctx context.Context, h agentx.Handler) error {
	c, err := agentx.Dial(ctx, "unix", "/var/agentx/master")
	if err != nil {
		return err
	}
	defer c.Close()

	s, err := c.Open(ctx, agentx.OID{1, 3, 6, 1, 4, 1, 99999}, "example", h)
	if err != nil {
		return err
	}
	if err := s.Register(ctx, agentx.OID{1, 3, 6, 1, 4, 1, 99999, 1}, agentx.DefaultPriority); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		s.Close()
		return ctx.Err()
	case <-s.Done():
		return s.Err()
	}
}

// 套件不會自行重連：session 結束（snmpd 重啟、斷線）後，由呼叫端決定何時重連。
func Example() {
	ctx := context.Background()
	h := uptime{start: time.Now()}
	for {
		err := serve(ctx, h)
		if ctx.Err() != nil {
			return
		}
		log.Printf("agentx session ended: %v; reconnecting in 5s", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}
}
