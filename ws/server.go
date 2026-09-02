package ws

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/TheChosenGay/combet"
	wslib "github.com/gorilla/websocket"
)

// Server 是 WebSocket 协议的 comet.Server 实现。
type Server struct {
	*comet.Core

	addr    string
	logger  *slog.Logger
	httpSrv *http.Server
	// ReadTimeout 连接读超时（心跳/任意帧重置；0 = 默认 120s）。超时后连接关闭，
	// 由业务层（如网关 sweeper）检测并踢线——心跳超时踢线靠这个可配置阈值。
	ReadTimeout time.Duration

	ready chan struct{}
}

// NewServer 创建 WebSocket comet server（便捷构造，内部创建 Core）。
func NewServer(addr string, cfg comet.ServerConfig) *Server {
	return NewServerWithCore(addr, comet.NewCore(cfg))
}

// NewServerWithCore 使用预先配置好的 Core 创建 WebSocket server。
func NewServerWithCore(addr string, core *comet.Core) *Server {
	return &Server{
		Core:   core,
		addr:   addr,
		logger: slog.With("component", "ws-server"),
		ready:  make(chan struct{}),
	}
}

// Addr 返回绑定后的地址（Start 后为实际端口）。
func (s *Server) Addr() string { return s.addr }

// Ready 在监听建立后关闭，用于测试等待服务就绪。
func (s *Server) Ready() <-chan struct{} { return s.ready }

var upgrader = wslib.Upgrader{
	ReadBufferSize:  4096,
	WriteBufferSize: 4096,
	CheckOrigin:     func(r *http.Request) bool { return true },
}

// Start 启动 WebSocket 服务器，阻塞直到 ctx 取消。
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/ws", s.handleWS)

	s.httpSrv = &http.Server{
		Handler: mux,
	}

	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return err
	}
	s.addr = ln.Addr().String()
	close(s.ready)

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("websocket server starting", "addr", s.addr)
		if err := s.httpSrv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		s.logger.Info("websocket server shutting down...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return s.httpSrv.Shutdown(shutdownCtx)
	case err := <-errCh:
		return err
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	rawWS, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("ws upgrade failed", "err", err, "remote", r.RemoteAddr)
		return
	}

	ctx := context.Background()

	var conn *Conn
	conn = New(rawWS, func(raw []byte) {
		s.Core.Dispatch(ctx, conn, raw)
	}, s.ReadTimeout)
	conn.SetLogger(s.logger.With("conn_id", conn.ID()))

	s.Core.ConnManager().Push(conn)
	s.logger.Info("connection established", "conn_id", conn.ID(), "addr", conn.Addr())

	go conn.WritePump()
	conn.ReadLoop()

	userID := s.Core.ConnManager().RoomOf(conn.ID())
	s.Core.ConnManager().Pop(conn)
	s.logger.Info("connection closed", "conn_id", conn.ID(), "user_id", userID)
}
