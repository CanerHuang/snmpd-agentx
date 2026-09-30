//go:build integration

package agentx

// 整合測試：用真的 net-snmp snmpd 當 master，再用 net-snmp 的指令查詢。
//
//	go test -tags integration -run Integration -v .
//
// 需要 snmpd 與 snmpget、snmpgetnext、snmpwalk、snmpbulkget、snmpbulkwalk、snmpset。
// snmpd 不在 PATH 時，用環境變數 AGENTX_SNMPD 指定路徑。

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

// snmpd 是測試用的 master，每個測試各自一份設定與埠號。
type snmpd struct {
	t       *testing.T
	bin     string
	dir     string
	conf    string
	target  string // SNMP 查詢的位址，例如 127.0.0.1:16161
	network string // AgentX 的 network 與 address
	address string
	cmd     *exec.Cmd
}

func freePort(t *testing.T, network string) int {
	t.Helper()
	switch network {
	case "udp":
		c, err := net.ListenPacket("udp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		return c.LocalAddr().(*net.UDPAddr).Port
	default:
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		return l.Addr().(*net.TCPAddr).Port
	}
}

func newSNMPD(t *testing.T, network string) *snmpd {
	t.Helper()
	bin := os.Getenv("AGENTX_SNMPD")
	if bin == "" {
		bin = "snmpd"
	}
	bin, err := exec.LookPath(bin)
	if err != nil {
		t.Skip("snmpd not found; set AGENTX_SNMPD")
	}
	for _, tool := range []string{"snmpget", "snmpgetnext", "snmpwalk", "snmpbulkget", "snmpbulkwalk", "snmpset"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s not found", tool)
		}
	}

	d := &snmpd{t: t, bin: bin, dir: t.TempDir(), network: network}
	d.target = fmt.Sprintf("127.0.0.1:%d", freePort(t, "udp"))
	var socket string
	switch network {
	case "unix":
		d.address = filepath.Join(d.dir, "agentx.sock")
		if len(d.address) >= 108 {
			t.Skipf("socket path too long for a unix socket: %s", d.address)
		}
		socket = d.address
	case "tcp":
		d.address = fmt.Sprintf("127.0.0.1:%d", freePort(t, "tcp"))
		socket = "tcp:" + d.address
	}
	// 社群的 view 只開放測試用的 subtree：走訪到尾端時一定是 endOfMibView，不會混到
	// snmpd 自己的 MIB。
	d.conf = filepath.Join(d.dir, "snmpd.conf")
	conf := fmt.Sprintf(`master agentx
agentXSocket %s
agentaddress udp:%s
rocommunity public 127.0.0.1 %s
rwcommunity private 127.0.0.1 %s
`, socket, d.target, base, base)
	if err := os.WriteFile(d.conf, []byte(conf), 0o644); err != nil {
		t.Fatal(err)
	}
	d.start()
	t.Cleanup(d.stop)
	return d
}

// start 啟動 snmpd，等 AgentX socket 可以連線才返回。
func (d *snmpd) start() {
	d.t.Helper()
	d.cmd = exec.Command(d.bin, "-f", "-C", "-c", d.conf, "-Lf", filepath.Join(d.dir, "snmpd.log"))
	d.cmd.Env = append(os.Environ(), "SNMP_PERSISTENT_DIR="+filepath.Join(d.dir, "persist"))
	if err := d.cmd.Start(); err != nil {
		d.t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, err := net.Dial(d.network, d.address)
		if err == nil {
			conn.Close()
			return
		}
		if time.Now().After(deadline) {
			log, _ := os.ReadFile(filepath.Join(d.dir, "snmpd.log"))
			d.t.Fatalf("snmpd did not open %s %s: %v\n%s", d.network, d.address, err, log)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (d *snmpd) stop() {
	if d.cmd == nil {
		return
	}
	d.cmd.Process.Signal(syscall.SIGTERM)
	d.cmd.Wait()
	d.cmd = nil
}

// run 執行 net-snmp 的指令，回傳 stdout 的每一行與 stderr。
func (d *snmpd) run(tool string, args ...string) ([]string, string) {
	d.t.Helper()
	community := "public"
	if tool == "snmpset" {
		community = "private"
	}
	full := append([]string{"-v2c", "-c", community, "-On", "-t", "2", "-r", "0", d.target}, args...)
	cmd := exec.Command(tool, full...)
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, _ := cmd.Output()
	var lines []string
	for l := range strings.Lines(string(out)) {
		if l = strings.TrimSpace(l); l != "" {
			lines = append(lines, l)
		}
	}
	return lines, stderr.String()
}

// connect 連上 snmpd、開 session 並註冊測試用的 subtree。
func (d *snmpd) connect() (*Client, *Session) {
	d.t.Helper()
	ctx := context.Background()
	c, err := Dial(ctx, d.network, d.address, WithLogger(testLogger(d.t)))
	if err != nil {
		d.t.Fatal(err)
	}
	s, err := c.Open(ctx, base, "integration test", &listHandler{vbs: testData()})
	if err != nil {
		d.t.Fatal(err)
	}
	if err := s.Register(ctx, sub(1), DefaultPriority); err != nil {
		d.t.Fatal(err)
	}
	return c, s
}

func line(o OID, value string) string { return "." + o.String() + " = " + value }

var (
	lineInt       = line(sub(1, 1, 0), "INTEGER: 42")
	lineStr       = line(sub(1, 2, 0), `STRING: "hello"`)
	lineCounter64 = line(sub(1, 3, 0), "Counter64: 1099511627776")
	lineTicks     = line(sub(1, 4, 0), "Timeticks: (9000) 0:01:30.00")
	// view 只到測試的 subtree，走訪到尾端時 net-snmp 會印出這一行。
	lineEnd = line(sub(1, 4, 0), "No more variables left in this MIB View (It is past the end of the MIB tree)")
)

func checkLines(t *testing.T, what string, got []string, stderr string, want ...string) {
	t.Helper()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("%s:\ngot:\n  %s\nwant:\n  %s\nstderr: %s", what, strings.Join(got, "\n  "), strings.Join(want, "\n  "), stderr)
	}
}

func TestIntegration(t *testing.T) {
	for _, network := range []string{"unix", "tcp"} {
		t.Run(network, func(t *testing.T) {
			d := newSNMPD(t, network)
			c, _ := d.connect()
			defer c.Close()
			testQueries(t, d)
		})
	}
}

func testQueries(t *testing.T, d *snmpd) {
	t.Helper()
	missing := sub(1, 9, 0)

	got, stderr := d.run("snmpget", sub(1, 1, 0).String(), sub(1, 2, 0).String(), missing.String(), sub(1, 4, 0).String())
	checkLines(t, "snmpget with 4 OIDs", got, stderr,
		lineInt, lineStr, line(missing, "No Such Instance currently exists at this OID"), lineTicks)

	got, stderr = d.run("snmpget", sub(1, 3, 0).String())
	checkLines(t, "snmpget with 1 OID", got, stderr, lineCounter64)

	got, stderr = d.run("snmpgetnext", sub(1, 1, 0).String(), sub(1, 3, 0).String(), base.String())
	checkLines(t, "snmpgetnext with 3 OIDs", got, stderr, lineStr, lineTicks, lineInt)

	got, stderr = d.run("snmpwalk", base.String())
	checkLines(t, "snmpwalk", got, stderr, lineInt, lineStr, lineCounter64, lineTicks, lineEnd)

	got, stderr = d.run("snmpbulkwalk", "-Cr2", base.String())
	checkLines(t, "snmpbulkwalk", got, stderr, lineInt, lineStr, lineCounter64, lineTicks, lineEnd)

	got, stderr = d.run("snmpbulkget", "-Cn1", "-Cr3", sub(1, 3, 0).String(), sub(1, 2, 0).String(), base.String())
	checkLines(t, "snmpbulkget", got, stderr,
		lineTicks,
		lineCounter64, lineInt,
		lineTicks, lineStr,
		lineEnd, lineCounter64)

	_, stderr = d.run("snmpset", sub(1, 1, 0).String(), "i", "5")
	if !strings.Contains(stderr, "notWritable") {
		t.Errorf("snmpset: want notWritable, got %q", stderr)
	}
}

// snmpd 重啟：session 會結束並帶出原因，重新連線、註冊之後查詢恢復正常。
func TestIntegrationRestart(t *testing.T) {
	d := newSNMPD(t, "unix")
	c, s := d.connect()
	defer c.Close()

	d.stop()
	select {
	case <-s.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("session did not end after snmpd stopped")
	}
	var sce *SessionClosedError
	if !errors.Is(s.Err(), io.EOF) && !errors.As(s.Err(), &sce) {
		t.Fatalf("session error = %v", s.Err())
	}
	t.Logf("session ended: %v", s.Err())

	d.start()
	c2, _ := d.connect()
	defer c2.Close()
	got, stderr := d.run("snmpget", sub(1, 1, 0).String(), sub(1, 2, 0).String())
	checkLines(t, "snmpget after restart", got, stderr, lineInt, lineStr)
}
