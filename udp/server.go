package udp

import (
	"context"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/TheChosenGay/combet"
)

// Server 是 UDP 协议的 comet.Server 实现。
type Server struct {
	*comet.Core

	addr   string
	cfg    Config
	logger *slog.Logger

	pc     net.PacketConn
	mu     sync.RWMutex
	byAddr map[string]*Conn

	ready chan struct{}
}

// NewServer 创建 UDP comet server（便捷构造，内部创建 Core）。
func NewServer(addr string, coreCfg comet.ServerConfig, cfg Config) *Server {
	return NewServerWithCore(addr, comet.NewCore(coreCfg), cfg)
}

// NewServerWithCore 使用预先配置好的 Core 创建 UDP server。
func NewServerWithCore(addr string, core *comet.Core, cfg Config) *Server {
	return &Server{
		Core:   core,
		addr:   addr,
		cfg:    cfg.withDefaults(),
		logger: slog.With("component", "udp-server"),
		byAddr: make(map[string]*Conn),
		ready:  make(chan struct{}),
	}
}

// Addr 返回绑定后的地址（Start 后为实际端口）。
func (s *Server) Addr() string { return s.addr }

// Ready 在监听建立后关闭，用于测试等待服务就绪。
func (s *Server) Ready() <-chan struct{} { return s.ready }

// Config 返回当前配置副本。
func (s *Server) Config() Config { return s.cfg }

// Start 启动 UDP 服务器，阻塞直到 ctx 取消。
func (s *Server) Start(ctx context.Context) error {
	pc, err := net.ListenPacket("udp", s.addr)
	if err != nil {
		return err
	}
	s.pc = pc
	s.addr = pc.LocalAddr().String()
	close(s.ready)

	s.logger.Info("udp server starting", "addr", s.addr)
	go s.readLoop(ctx)
	go s.gcLoop(ctx)

	<-ctx.Done()
	s.shutdown()
	return pc.Close()
}

// readLoop 是单一读循环：从共享 socket 读 datagram，按源地址路由到会话。
func (s *Server) readLoop(ctx context.Context) {
	buf := make([]byte, s.cfg.MaxSegmentSize+1+fragHeader)
	for {
		n, addr, err := s.pc.ReadFrom(buf)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.logger.Error("read error", "err", err)
			continue
		}

		// 必须拷贝：buf 被复用，而读到的字节会投递到会话的 process goroutine 异步消费；
		// 若直接把 buf[:n] 交给它，下一次 ReadFrom 会覆盖上一个还没处理完的帧，造成跨
		// goroutine 数据竞争。若要避免拷贝，可用 sync.Pool + 交给 consumer 释放。
		pkt := make([]byte, n)
		copy(pkt, buf[:n])

		conn := s.getOrCreate(addr)
		if conn == nil {
			continue
		}
		conn.HandlePacket(pkt)
	}
}

// getOrCreate 按源地址查找会话，不存在则建立并登记。
func (s *Server) getOrCreate(addr net.Addr) *Conn {
	key := addr.String()

	s.mu.RLock()
	if c, ok := s.byAddr[key]; ok {
		s.mu.RUnlock()
		return c
	}
	s.mu.RUnlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.byAddr[key]; ok {
		return c
	}

	c := NewConn(s.pc, addr, s.cfg)
	c.SetOnFrame(func(frame []byte) {
		s.Core.Dispatch(context.Background(), c, frame)
	})
	c.SetOnClose(func() {
		s.remove(c)
	})
	s.byAddr[key] = c
	s.ConnManager().Push(c)
	c.Start()
	return c
}

// remove 从路由表和 ConnManager 移除会话。幂等。
func (s *Server) remove(c *Conn) {
	key := c.addr.String()
	s.mu.Lock()
	if s.byAddr[key] == c {
		delete(s.byAddr, key)
	}
	s.mu.Unlock()
	s.ConnManager().Pop(c)
}

func (s *Server) gcLoop(ctx context.Context) {
	t := time.NewTicker(s.cfg.GCInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.gcOnce()
		}
	}
}

func (s *Server) gcOnce() {
	now := time.Now()
	var closeList []*Conn

	s.mu.RLock()
	for _, c := range s.byAddr {
		c.reassembler.GC(now, s.cfg.ReasmTTL)
		if s.expired(c, now) {
			closeList = append(closeList, c)
		}
	}
	s.mu.RUnlock()

	// 不能在持有 RLock 时调用 c.Close()：onClose→remove 需要获取 s.mu.Lock()，
	// 在持读锁时升级写锁会发生 RWMutex 阻塞死锁。所以先快照，释放锁后再关闭。
	for _, c := range closeList {
		_ = c.Close() // onClose 会 remove(byAddr) + ConnManager.Pop
	}
}

func (s *Server) expired(c *Conn, now time.Time) bool {
	last := c.LastActive()
	if now.Sub(last) > s.cfg.IdleTimeout {
		return true
	}
	// 未鉴权判定：既没绑定房间、也没进入握手完成态。
	authed := s.ConnManager().RoomOf(c.ID()) != "" || s.ConnManager().IsHandshaken(c.ID())
	if !authed && now.Sub(last) > s.cfg.UnauthedTTL {
		return true
	}
	return false
}

func (s *Server) shutdown() {
	s.mu.RLock()
	all := make([]*Conn, 0, len(s.byAddr))
	for _, c := range s.byAddr {
		all = append(all, c)
	}
	s.mu.RUnlock()
	// 同样不能持 RLock 去 Close（会在 remove 里卡在升级写锁上）。
	for _, c := range all {
		_ = c.Close()
	}
}

var _ comet.Server = (*Server)(nil)
