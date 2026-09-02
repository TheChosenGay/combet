package ws_test

import (
	"context"
	"io"
	"log/slog"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TheChosenGay/combet"
	"github.com/TheChosenGay/combet/client"
	"github.com/TheChosenGay/combet/ws"
)

func init() {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

type echoBiz struct {
	core *comet.Core
}

func (b *echoBiz) OnAuth(_ context.Context, _ []byte) (string, error) {
	return "u1", nil
}

func (b *echoBiz) OnMessage(_ context.Context, connID, _ string, payload []byte) error {
	b.core.Send(connID, &comet.Msg{Type: comet.MsgData, Payload: append([]byte(nil), payload...)})
	return nil
}

func newWSServer(tb testing.TB, biz *echoBiz) (*ws.Server, func()) {
	tb.Helper()
	core := comet.NewCore(comet.ServerConfig{Business: biz})
	biz.core = core
	srv := ws.NewServerWithCore("127.0.0.1:0", core)
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Start(ctx) }()
	select {
	case <-srv.Ready():
	case err := <-errCh:
		tb.Fatalf("ws start failed: %v", err)
	}
	return srv, cancel
}

// BenchmarkWSEcho 测量：并发 ws 客户端 ping-pong，端到端往返吞吐。
func BenchmarkWSEcho(b *testing.B) {
	srv, cancel := newWSServer(b, &echoBiz{})
	defer cancel()

	procs := runtime.GOMAXPROCS(0)
	conns := make([]*client.Conn, procs)
	chs := make([]chan []byte, procs)
	for i := range conns {
		c, err := client.Dial(context.Background(), "ws://"+srv.Addr()+"/ws", "token")
		if err != nil {
			b.Fatal(err)
		}
		conns[i] = c
		ch := make(chan []byte, 1)
		chs[i] = ch
		c.OnMessage(func(p []byte) { ch <- p })
		go c.WritePump()
		go c.ReadLoop()
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
			<-ch
		}
	})
}

// BenchmarkWSBroadcast 测量：服务端向固定房间广播时的 Push 吞吐。
func BenchmarkWSBroadcast(b *testing.B) {
	srv, cancel := newWSServer(b, &echoBiz{})
	defer cancel()

	const n = 16
	for i := 0; i < n; i++ {
		c, err := client.Dial(context.Background(), "ws://"+srv.Addr()+"/ws", "token")
		if err != nil {
			b.Fatal(err)
		}
		c.OnMessage(func([]byte) {})
		go c.WritePump()
		go c.ReadLoop()
		defer c.Close()
	}

	payload := []byte("broadcast payload")
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		srv.Push("u1", payload)
	}
}

// TestWSGoroutineLeak 验证：建立并关闭若干 ws 连接并关服后，goroutine 回归基线。
func TestWSGoroutineLeak(t *testing.T) {
	base := runtime.NumGoroutine()
	srv, cancel := newWSServer(t, &echoBiz{})

	const n = 16
	conns := make([]*client.Conn, n)
	for i := range conns {
		c, err := client.Dial(context.Background(), "ws://"+srv.Addr()+"/ws", "token")
		if err != nil {
			t.Fatal(err)
		}
		c.OnMessage(func([]byte) {})
		go c.WritePump()
		go c.ReadLoop()
		conns[i] = c
	}

	for _, c := range conns {
		_ = c.Close()
	}
	cancel()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if runtime.NumGoroutine() <= base+5 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("goroutine leak: base=%d now=%d", base, runtime.NumGoroutine())
}
