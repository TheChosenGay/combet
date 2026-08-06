// Package ws 提供 WebSocket 协议的 Conn 实现。
// ws.Conn 实现了 comet.Conn 接口，读写各一个 goroutine。
package ws

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var (
	ErrSendFull   = errors.New("ws: send buffer full")
	ErrConnClosed = errors.New("ws: connection closed")
)

const defaultSendBuf = 256

// Conn 是 WebSocket 的 comet.Conn 实现。
// 读写分离：ReadLoop 独占读，WritePump 独占写。
type Conn struct {
	id     string
	ws     *websocket.Conn
	onRead func([]byte)
	logger *slog.Logger

	send   chan []byte
	ctx    context.Context
	cancel context.CancelFunc

	closedMu sync.Mutex
	closed   bool
}

// New 创建 WebSocket Conn。
// onRead 在读到完整帧时被调用（在 ReadLoop goroutine 中）。
func New(ws *websocket.Conn, onRead func([]byte)) *Conn {
	ctx, cancel := context.WithCancel(context.Background())
	return &Conn{
		id:     uuid.NewString(),
		ws:     ws,
		onRead: onRead,
		logger: slog.With("component", "ws"),
		send:   make(chan []byte, defaultSendBuf),
		ctx:    ctx,
		cancel: cancel,
	}
}

// ID 返回连接唯一标识。
func (c *Conn) ID() string { return c.id }

// Addr 返回远端地址。
func (c *Conn) Addr() string { return c.ws.RemoteAddr().String() }

// SetLogger 设置 logger（带 conn_id 等上下文）。
func (c *Conn) SetLogger(l *slog.Logger) { c.logger = l }

// Write 异步投递消息到写缓冲通道。
func (c *Conn) Write(ctx context.Context, data []byte) error {
	c.closedMu.Lock()
	if c.closed {
		c.closedMu.Unlock()
		return ErrConnClosed
	}
	c.closedMu.Unlock()

	select {
	case c.send <- data:
		return nil
	default:
		return ErrSendFull
	}
}

// ReadLoop 阻塞读 WebSocket，解析帧后回调 onRead。
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

		c.onRead(raw)
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
