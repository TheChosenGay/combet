package udp_test

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"runtime"
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
