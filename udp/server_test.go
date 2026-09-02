package udp_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TheChosenGay/combet"
	"github.com/TheChosenGay/combet/client"
	"github.com/TheChosenGay/combet/udp"
)

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// testBiz 实现 comet.Business。echo=true 时把消息原样回给发送者。
type testBiz struct {
	core *comet.Core
	echo bool
}

func (b *testBiz) OnAuth(_ context.Context, _ []byte) (string, error) {
	return "u1", nil
}

func (b *testBiz) OnMessage(_ context.Context, connID, _ string, payload []byte) error {
	if b.echo {
		b.core.Send(connID, &comet.Msg{Type: comet.MsgData, Payload: append([]byte(nil), payload...)})
	}
	return nil
}

func newTestServer(tb testing.TB, biz *testBiz, cfg udp.Config) (*udp.Server, func()) {
	tb.Helper()
	core := comet.NewCore(comet.ServerConfig{Business: biz})
	biz.core = core
	srv := udp.NewServerWithCore("127.0.0.1:0", core, cfg)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Start(ctx)
	}()
	select {
	case <-srv.Ready():
	case err := <-errCh:
		tb.Fatalf("server start failed: %v", err)
	}
	return srv, cancel
}

func TestUDPEcho(t *testing.T) {
	srv, cancel := newTestServer(t, &testBiz{echo: true}, udp.Config{})
	defer cancel()

	c, err := client.DialUDP(context.Background(), srv.Addr(), "token")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	got := make(chan []byte, 1)
	c.OnMessage(func(p []byte) { got <- p })

	if err := c.Send([]byte("hello")); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		if string(p) != "hello" {
			t.Fatalf("echo = %q", p)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting echo")
	}
}

func TestUDPBroadcast(t *testing.T) {
	srv, cancel := newTestServer(t, &testBiz{}, udp.Config{})
	defer cancel()

	const n = 3
	received := make(chan []byte, n)
	clients := make([]*client.UDPConn, n)
	for i := range clients {
		c, err := client.DialUDP(context.Background(), srv.Addr(), "token")
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = c
		defer c.Close()
		c.OnMessage(func(p []byte) { received <- p })
	}

	// DialUDP 已同步完成鉴权，服务端已 Bind 到房间 u1。
	delivered := srv.Push("u1", []byte("broadcast"))
	if delivered != n {
		t.Fatalf("delivered = %d, want %d", delivered, n)
	}

	for i := 0; i < n; i++ {
		select {
		case p := <-received:
			if string(p) != "broadcast" {
				t.Fatalf("got %q", p)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timeout waiting broadcast")
		}
	}
}

func TestUDPFragmentEcho(t *testing.T) {
	srv, cancel := newTestServer(t, &testBiz{echo: true}, udp.Config{})
	defer cancel()

	c, err := client.DialUDP(context.Background(), srv.Addr(), "token")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	got := make(chan []byte, 1)
	c.OnMessage(func(p []byte) { got <- p })

	// 10KB > MaxSegmentSize(1400)，必然分片 + 服务端重组。
	payload := bytes.Repeat([]byte("A"), 10000)
	if err := c.Send(payload); err != nil {
		t.Fatal(err)
	}
	select {
	case p := <-got:
		if !bytes.Equal(p, payload) {
			t.Fatalf("echo length = %d, want %d", len(p), len(payload))
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting fragmented echo")
	}
}

func TestUDPIdleReapUnauthed(t *testing.T) {
	cfg := udp.Config{
		UnauthedTTL: 200 * time.Millisecond,
		GCInterval:  50 * time.Millisecond,
		IdleTimeout: 30 * time.Second,
	}
	srv, cancel := newTestServer(t, &testBiz{}, cfg)
	defer cancel()

	// 未鉴权：发一个垃圾 datagram，触发建会话但不会鉴权。
	conn, err := net.Dial("udp", srv.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := conn.Write([]byte{0xde, 0xad}); err != nil {
		t.Fatal(err)
	}

	waitFor(t, func() bool { return srv.ConnCount() == 1 })
	waitFor(t, func() bool { return srv.ConnCount() == 0 })
}

func TestUDPGoroutineLeak(t *testing.T) {
	base := runtime.NumGoroutine()
	srv, cancel := newTestServer(t, &testBiz{echo: true}, udp.Config{})

	const n = 16
	clients := make([]*client.UDPConn, n)
	for i := range clients {
		c, err := client.DialUDP(context.Background(), srv.Addr(), "token")
		if err != nil {
			t.Fatal(err)
		}
		c.OnMessage(func([]byte) {})
		clients[i] = c
	}

	for _, c := range clients {
		_ = c.Close()
	}
	cancel() // 触发 shutdown，服务端显式关闭全部会话

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= base+5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: base=%d now=%d", base, runtime.NumGoroutine())
}

// TestUDPPacketLossSnapshotSurvival 模拟 10% datagram 丢失下，大快照(分片)与小增量(单包)的存活率。
// 说明：UDP 分片是"尽力而为"，丢一片即整条失败；小单包则各自独立。
func TestUDPPacketLossSnapshotSurvival(t *testing.T) {
	const (
		maxSeg = 1400
		msgs   = 200
		drop   = 0.10
	)
	rng := rand.New(rand.NewSource(42))

	big := bytes.Repeat([]byte("A"), 10000) // 10KB 快照，分 ~8 片
	small := bytes.Repeat([]byte("b"), 30)  // 30B 增量，单包

	bigOK, smallOK := 0, 0
	for m := 0; m < msgs; m++ {
		// 大快照：每片独立丢，丢任何一片则该消息不完整
		bd := udp.PackFrame(big, maxSeg, uint16(m*2))
		br := udp.NewReassembler(64<<10, maxSeg, 8)
		completed := false
		for _, d := range bd {
			if rng.Float64() < drop {
				continue
			}
			if _, ok := br.Feed(d); ok {
				completed = true
				break
			}
		}
		if completed {
			bigOK++
		}

		// 小增量：单包，独立丢失
		sd := udp.PackFrame(small, maxSeg, uint16(m*2+1))
		if rng.Float64() >= drop {
			if _, ok := udp.NewReassembler(64<<10, maxSeg, 8).Feed(sd[0]); ok {
				smallOK++
			}
		}
	}

	t.Logf("10%% loss: big(10KB fragmented) survived %d/%d, small(30B single) survived %d/%d",
		bigOK, msgs, smallOK, msgs)
}

// TestUDPHighConnectionLeak 在高并发下建立 10000 条 UDP 会话，验证容量与不泄漏。
// 用 -short 可跳过（默认 go test 会长跑几秒）。
func TestUDPHighConnectionLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10k connection leak test in -short mode")
	}
	const n = 10000

	base := runtime.NumGoroutine()
	srv, cancel := newTestServer(t, &testBiz{}, udp.Config{})
	defer cancel()

	clients := make([]*client.UDPConn, n)
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		dialErr error
	)
	sem := make(chan struct{}, 256) // 控制并发建连数
	for i := 0; i < n; i++ {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			c, err := client.DialUDP(context.Background(), srv.Addr(), "token")
			if err != nil {
				mu.Lock()
				if dialErr == nil {
					dialErr = err
				}
				mu.Unlock()
				return
			}
			c.OnMessage(func([]byte) {})
			clients[i] = c
		}(i)
	}
	wg.Wait()
	if dialErr != nil {
		t.Fatalf("dial failed: %v", dialErr)
	}
	if got := srv.ConnCount(); got != n {
		t.Fatalf("ConnCount = %d, want %d", got, n)
	}

	// 关闭全部客户端 + 关服（服务端 shutdown 显式关闭所有会话）
	for _, c := range clients {
		_ = c.Close()
	}
	cancel()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= base+20 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("goroutine leak after %d conns: base=%d now=%d", n, base, runtime.NumGoroutine())
}

// TestUDPRampUpTransferLeak 分批渐增连接直到 10000，每批校验连接数，
// 全部建立后并发让所有连接传输数据（send + echo 校验），最后关服检查泄漏。
// 用 -short 可跳过。
func TestUDPRampUpTransferLeak(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping 10k ramp-up transfer leak test in -short mode")
	}
	const (
		total   = 10000
		batch   = 1000
		workers = 256
	)

	base := runtime.NumGoroutine()
	srv, cancel := newTestServer(t, &testBiz{echo: true}, udp.Config{})
	defer cancel()

	clients := make([]*client.UDPConn, total)
	chs := make([]chan []byte, total)

	// 分批并发建连，逐渐增加连接量
	for off := 0; off < total; off += batch {
		end := min(off+batch, total)
		var (
			wg      sync.WaitGroup
			mu      sync.Mutex
			dialErr error
		)
		sem := make(chan struct{}, workers)
		for i := off; i < end; i++ {
			sem <- struct{}{}
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				defer func() { <-sem }()
				c, err := client.DialUDP(context.Background(), srv.Addr(), "token")
				if err != nil {
					mu.Lock()
					if dialErr == nil {
						dialErr = err
					}
					mu.Unlock()
					return
				}
				ch := make(chan []byte, 1)
				c.OnMessage(func(p []byte) { ch <- p })
				clients[i] = c
				chs[i] = ch
			}(i)
		}
		wg.Wait()
		if dialErr != nil {
			t.Fatalf("dial batch [%d,%d) failed: %v", off, end, dialErr)
		}
		if got := srv.ConnCount(); got != end {
			t.Fatalf("after batch up to %d: ConnCount=%d want %d", end, got, end)
		}
	}

	// 全部建立：并发让所有连接传输数据并校验 echo
	if err := echoAll(clients, chs, 0, total, workers); err != nil {
		t.Fatalf("echo all failed: %v", err)
	}

	// 关闭全部客户端 + 关服
	for _, c := range clients {
		_ = c.Close()
	}
	cancel()

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= base+20 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("goroutine leak after %d ramp-up conns: base=%d now=%d", total, base, runtime.NumGoroutine())
}

func echoAll(clients []*client.UDPConn, chs []chan []byte, from, to, workers int) error {
	payload := []byte("ping-batch")
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	sem := make(chan struct{}, workers)
	for i := from; i < to; i++ {
		sem <- struct{}{}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := clients[i].Send(payload); err != nil {
				mu.Lock()
				if firstErr == nil {
					firstErr = err
				}
				mu.Unlock()
				return
			}
			select {
			case p := <-chs[i]:
				if string(p) != string(payload) {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("echo mismatch at %d: %q", i, p)
					}
					mu.Unlock()
				}
			case <-time.After(5 * time.Second):
				mu.Lock()
				if firstErr == nil {
					firstErr = fmt.Errorf("timeout waiting echo at %d", i)
				}
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return firstErr
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// BenchmarkUDPThroughput 测量：并发客户端往服务端发消息（服务端 echo），
// 以每秒请求数表示端到端往返吞吐（ping-pong，天然背压，不会撑爆写队列）。
func BenchmarkUDPThroughput(b *testing.B) {
	srv, cancel := newTestServer(b, &testBiz{echo: true}, udp.Config{})
	defer cancel()

	procs := runtime.GOMAXPROCS(0)
	conns := make([]*client.UDPConn, procs)
	chs := make([]chan []byte, procs)
	for i := range conns {
		c, err := client.DialUDP(context.Background(), srv.Addr(), "token")
		if err != nil {
			b.Fatal(err)
		}
		conns[i] = c
		ch := make(chan []byte, 1)
		chs[i] = ch
		c.OnMessage(func(p []byte) { ch <- p })
		defer c.Close()
	}

	payload := []byte("hello world")
	var next uint32
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := int(atomic.AddUint32(&next, 1)-1) % procs
		c := conns[i]
		ch := chs[i]
		for pb.Next() {
			if err := c.Send(payload); err != nil {
				b.Errorf("send: %v", err)
				return
			}
			<-ch // 等待 echo 回来再发下一条
		}
	})
}

// BenchmarkUDPBroadcast 测量：服务端向固定房间广播时的 Push 吞吐。
func BenchmarkUDPBroadcast(b *testing.B) {
	srv, cancel := newTestServer(b, &testBiz{}, udp.Config{})
	defer cancel()

	const n = 16
	for i := 0; i < n; i++ {
		c, err := client.DialUDP(context.Background(), srv.Addr(), "token")
		if err != nil {
			b.Fatal(err)
		}
		c.OnMessage(func([]byte) {})
		defer c.Close()
	}

	payload := []byte("broadcast payload")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.Push("u1", payload)
	}
}
