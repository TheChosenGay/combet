// Package client 提供 comet 客户端连接实现。
// 用于服务端之间通信或其他需要连接 comet 的场景。
package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"time"

	"github.com/TheChosenGay/combet"
	"github.com/gorilla/websocket"
)

var (
	ErrAuthFailed = errors.New("client: auth failed")
	ErrConnClosed = errors.New("client: connection closed")
	ErrSendFull   = errors.New("client: send buffer full")
)

const (
	defaultSendBuf  = 256
	dialTimeout     = 10 * time.Second
	authWaitTimeout = 5 * time.Second
)

// scheme 是客户端使用的协议方案（与服务端保持一致；默认帧协议）。
var scheme = comet.NewFrameScheme()

// Conn 是 comet 客户端连接。
// 自动处理鉴权握手、心跳响应。
type Conn struct {
	ws *websocket.Conn
	id string

	send   chan []byte
	ctx    context.Context
	cancel context.CancelFunc

	onMessage func([]byte) // 收到业务消息的回调

	closedMu sync.Mutex
	closed   bool

	logger *slog.Logger
}

// Dial 连接到 comet 服务端，完成鉴权握手。
// addr 例如 "ws://localhost:8081/ws"，token 是 JWT。
func Dial(ctx context.Context, addr string, token string) (*Conn, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, fmt.Errorf("client: parse addr: %w", err)
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	ws, _, err := websocket.DefaultDialer.DialContext(dialCtx, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("client: dial: %w", err)
	}

	connCtx, connCancel := context.WithCancel(context.Background())
	c := &Conn{
		ws:     ws,
		send:   make(chan []byte, defaultSendBuf),
		ctx:    connCtx,
		cancel: connCancel,
		logger: slog.With("component", "comet-client"),
	}

	authFrame, err := scheme.Encode(&comet.Msg{Type: comet.MsgHandshakeReq, Payload: []byte(token)})
	if err != nil {
		ws.Close()
		connCancel()
		return nil, fmt.Errorf("client: encode auth: %w", err)
	}
	if err := ws.WriteMessage(websocket.BinaryMessage, authFrame); err != nil {
		ws.Close()
		connCancel()
		return nil, fmt.Errorf("client: send auth: %w", err)
	}

	// 等待鉴权应答
	ws.SetReadDeadline(time.Now().Add(authWaitTimeout))
	_, raw, err := ws.ReadMessage()
	if err != nil {
		ws.Close()
		connCancel()
		return nil, fmt.Errorf("client: read auth reply: %w", err)
	}

	reply, err := scheme.Decode(raw)
	if err != nil ||
		reply.Type != comet.MsgHandshakeResp ||
		len(reply.Payload) == 0 ||
		reply.Payload[0] != 1 {
		ws.Close()
		connCancel()
		return nil, ErrAuthFailed
	}

	c.logger.Info("authenticated", "addr", addr)
	return c, nil
}

// Send 发送业务消息。
func (c *Conn) Send(payload []byte) error {
	c.closedMu.Lock()
	if c.closed {
		c.closedMu.Unlock()
		return ErrConnClosed
	}
	c.closedMu.Unlock()

	frame, err := scheme.Encode(&comet.Msg{Type: comet.MsgData, Payload: payload})
	if err != nil {
		return err
	}

	select {
	case c.send <- frame:
		return nil
	default:
		return ErrSendFull
	}
}

// OnMessage 注册业务消息回调。
func (c *Conn) OnMessage(fn func([]byte)) {
	c.onMessage = fn
}

// ReadLoop 阻塞读消息，处理心跳，业务消息回调 onMessage。
func (c *Conn) ReadLoop() error {
	defer c.Close()

	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		if err := c.ws.SetReadDeadline(time.Now().Add(120 * time.Second)); err != nil {
			return err
		}

		_, raw, err := c.ws.ReadMessage()
		if err != nil {
			return err
		}

		msg, err := scheme.Decode(raw)
		if err != nil {
			c.logger.Warn("decode failed", "err", err)
			continue
		}

		switch msg.Type {
		case comet.MsgHeartbeatReq:
			// 服务端心跳 → 直接回复
			c.sendHeartbeatReply()

		case comet.MsgData:
			if c.onMessage != nil {
				c.onMessage(msg.Payload)
			}

		default:
			c.logger.Warn("unknown message type", "type", msg.Type)
		}
	}
}

// WritePump 从 send 通道消费，写入 WebSocket。
func (c *Conn) WritePump() {
	defer c.Close()

	for {
		select {
		case <-c.ctx.Done():
			return
		case msg, ok := <-c.send:
			if !ok {
				return
			}
			if err := c.ws.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
				return
			}
			if err := c.ws.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				return
			}
		}
	}
}

// Close 关闭连接。
func (c *Conn) Close() error {
	c.closedMu.Lock()
	defer c.closedMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true
	c.cancel()
	return c.ws.Close()
}

func (c *Conn) sendHeartbeatReply() {
	reply, err := scheme.Encode(&comet.Msg{Type: comet.MsgHeartbeatAck})
	if err != nil {
		return
	}
	select {
	case c.send <- reply:
	default:
	}
}
