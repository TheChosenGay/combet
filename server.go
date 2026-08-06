package comet

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const tracerName = "github.com/TheChosenGay/combet"

// Server 是协议无关的 comet server 接口。
type Server interface {
	Start(ctx context.Context) error
	Push(roomID string, data []byte) int
	ConnCount() int
}

// Pusher 消息推送能力。Core 实现此接口。
// 业务层通过此接口推送消息，避免直接依赖 *Core。
type Pusher interface {
	Push(roomID string, data []byte) int
}

// ServerConfig 创建 Core 的依赖注入。
type ServerConfig struct {
	Business    Business
	ConnManager *ConnManager
	// Scheme 协议编解码方案；nil 时使用默认帧协议（[2B type][payload]）。
	// 换协议（如 pomelo）只需实现 MsgScheme 并注入。
	Scheme MsgScheme
}

// ============================================================
// Core — 协议无关的共享 dispatch 逻辑
// ============================================================

// Core 封装了消息分发、鉴权、心跳、推送等协议无关逻辑。
// 各协议实现（ws.Server / tcp.Server）嵌入 *Core 复用。
type Core struct {
	cfg    ServerConfig
	scheme MsgScheme
	tracer trace.Tracer
	logger *slog.Logger
}

// NewCore 创建共享 dispatch 核心。
func NewCore(cfg ServerConfig) *Core {
	if cfg.ConnManager == nil {
		cfg.ConnManager = NewConnManager()
	}
	if cfg.Scheme == nil {
		cfg.Scheme = NewFrameScheme()
	}
	return &Core{
		cfg:    cfg,
		scheme: cfg.Scheme,
		tracer: otel.Tracer(tracerName),
		logger: slog.With("component", "comet"),
	}
}

// ConnManager 返回内部的 ConnManager。
func (c *Core) ConnManager() *ConnManager {
	return c.cfg.ConnManager
}

// Push 向指定房间广播消息。
func (c *Core) Push(roomID string, data []byte) int {
	ctx, span := c.tracer.Start(context.Background(), "server.push",
		trace.WithAttributes(attribute.String("room_id", roomID)))
	defer span.End()

	frame := append(TypeMessage[:], data...)
	delivered := c.cfg.ConnManager.PushToRoom(roomID, frame)
	span.SetAttributes(attribute.Int("delivered", delivered))
	_ = ctx
	return delivered
}

// ConnCount 返回当前连接总数。
func (c *Core) ConnCount() int {
	return c.cfg.ConnManager.ConnCount()
}

// RoomOnline 查询房间在线状态。
func (c *Core) RoomOnline(roomID string) (bool, int) {
	return c.cfg.ConnManager.RoomOnline(roomID)
}

// Dispatch 消息分发。协议实现的 onRead 回调应调用此方法。
// 原始字节先经 Scheme.Decode 转成语义消息，再按语义类型分发——
// 协议差异被隔离在 MsgScheme 内，Core 不感知具体线路格式。
func (c *Core) Dispatch(ctx context.Context, conn Conn, raw []byte) {
	msg, err := c.scheme.Decode(raw)
	if err != nil {
		c.logger.Warn("decode failed", "conn_id", conn.ID(), "err", err)
		return
	}

	switch msg.Type {
	case MsgHeartbeatReq:
		c.handleHeartbeat(ctx, conn)

	case MsgHandshakeReq:
		ctx, span := c.tracer.Start(ctx, "ws.auth",
			trace.WithAttributes(attribute.String("conn_id", conn.ID())))
		defer span.End()
		c.handleAuth(ctx, conn, msg.Payload)

	case MsgData:
		ctx, span := c.tracer.Start(ctx, "ws.message",
			trace.WithAttributes(
				attribute.String("conn_id", conn.ID()),
				attribute.Int("payload_size", len(msg.Payload)),
			))
		defer span.End()
		c.handleMessage(ctx, conn, msg.Payload)

	default:
		c.logger.Warn("unhandled msg type", "type", msg.Type, "conn_id", conn.ID())
	}
}

// write 把语义消息经 Scheme.Encode 编码后写入连接。
func (c *Core) write(conn Conn, msg *Msg) error {
	data, err := c.scheme.Encode(msg)
	if err != nil {
		c.logger.Warn("encode failed", "conn_id", conn.ID(), "err", err)
		return err
	}
	return conn.Write(context.Background(), data)
}

// ============================================================
// 内部方法
// ============================================================

func (c *Core) handleHeartbeat(ctx context.Context, conn Conn) {
	if err := c.write(conn, &Msg{Type: MsgHeartbeatAck}); err != nil {
		c.logger.Warn("heartbeat write failed", "conn_id", conn.ID(), "err", err)
	}
}

func (c *Core) handleAuth(ctx context.Context, conn Conn, payload []byte) {
	userID, err := c.cfg.Business.OnAuth(ctx, payload)
	if err != nil || userID == "" {
		c.logger.Warn("auth failed", "conn_id", conn.ID(), "err", err)
		span := trace.SpanFromContext(ctx)
		span.SetStatus(codes.Error, "auth failed")
		if err != nil {
			span.RecordError(err)
		}
		c.write(conn, &Msg{Type: MsgHandshakeResp, Payload: []byte{0x00}}) // 失败
		return
	}

	// 绑定到房间（roomID = userID）
	c.cfg.ConnManager.Bind(userID, conn)

	trace.SpanFromContext(ctx).SetAttributes(attribute.String("user_id", userID))
	c.logger.Info("auth success", "conn_id", conn.ID(), "user_id", userID)
	c.write(conn, &Msg{Type: MsgHandshakeResp, Payload: []byte{0x01}}) // 成功
}

func (c *Core) handleMessage(ctx context.Context, conn Conn, payload []byte) {
	userID := c.cfg.ConnManager.RoomOf(conn.ID())
	if userID == "" {
		c.logger.Warn("message from unauthenticated conn", "conn_id", conn.ID())
		trace.SpanFromContext(ctx).SetStatus(codes.Error, "unauthenticated")
		return
	}

	span := trace.SpanFromContext(ctx)
	span.SetAttributes(attribute.String("user_id", userID))

	if err := c.cfg.Business.OnMessage(ctx, conn.ID(), userID, payload); err != nil {
		c.logger.Warn("onMessage error", "conn_id", conn.ID(), "user_id", userID, "err", err)
		span.SetStatus(codes.Error, "onMessage failed")
		span.RecordError(err)
	}
}
