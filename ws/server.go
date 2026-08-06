package ws

import (
	"context"
	"errors"
	"log/slog"
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
	}
}

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
		Addr:    s.addr,
		Handler: mux,
	}

	errCh := make(chan error, 1)
	go func() {
		s.logger.Info("websocket server starting", "addr", s.addr)
		if err := s.httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
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
	})
	conn.SetLogger(s.logger.With("conn_id", conn.ID()))

	s.Core.ConnManager().Push(conn)
	s.logger.Info("connection established", "conn_id", conn.ID(), "addr", conn.Addr())

	go conn.WritePump()
	conn.ReadLoop()

	userID := s.Core.ConnManager().RoomOf(conn.ID())
	s.Core.ConnManager().Pop(conn)
	s.logger.Info("connection closed", "conn_id", conn.ID(), "user_id", userID)
}
