package client

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/TheChosenGay/combet"
	"github.com/TheChosenGay/combet/udp"
)

var (
	ErrUDPAuthFailed = errors.New("client: udp auth failed")
	ErrUDPConnClosed = errors.New("client: udp connection closed")
	ErrUDPSendFull   = errors.New("client: udp send buffer full")
)

const (
	udpSendBuf     = 256
	udpDialTimeout = 5 * time.Second
	udpAuthTimeout = 5 * time.Second
	udpReadTimeout = 120 * time.Second
	udpMaxSeg      = 1400
)

// UDPConn 是 comet 的 UDP 客户端连接。
// 使用 connected UDP socket（net.Dial），一次只跟一个服务端通信。
// 自动发送鉴权帧（MsgHandshakeReq），并在 ReadLoop 里处理心跳/业务消息，
// 发送侧与接收侧都走 udp 的分片/重组。
type UDPConn struct {
	conn net.Conn

	send   chan []byte
	ctx    context.Context
	cancel context.CancelFunc

	onMessage func([]byte)
	msgMu     sync.Mutex

	reassembler *udp.Reassembler
	msgID       uint16 // 仅 WritePump goroutine 访问

	closedMu sync.Mutex
	closed   bool

	logger *slog.Logger
}

// DialUDP 连接 comet UDP 服务端并完成鉴权握手。
// addr 例如 "127.0.0.1:8081"，token 为鉴权负载。
func DialUDP(ctx context.Context, addr, token string) (*UDPConn, error) {
	d := net.Dialer{Timeout: udpDialTimeout}
	uconn, err := d.DialContext(ctx, "udp", addr)
	if err != nil {
		return nil, fmt.Errorf("client: udp dial: %w", err)
	}

	connCtx, connCancel := context.WithCancel(context.Background())
	c := &UDPConn{
		conn:        uconn,
		send:        make(chan []byte, udpSendBuf),
		ctx:         connCtx,
		cancel:      connCancel,
		reassembler: udp.NewReassembler(64<<10, udpMaxSeg, 64),
		logger:      slog.With("component", "udp-client"),
	}

	// 鉴权帧
	authFrame, err := scheme.Encode(&comet.Msg{Type: comet.MsgHandshakeReq, Payload: []byte(token)})
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("client: encode auth: %w", err)
	}
	dgrams := udp.PackFrame(authFrame, udpMaxSeg, 1)
	if _, err := uconn.Write(dgrams[0]); err != nil {
		c.Close()
		return nil, fmt.Errorf("client: send auth: %w", err)
	}

	// 读鉴权应答
	if err := uconn.SetReadDeadline(time.Now().Add(udpAuthTimeout)); err != nil {
		c.Close()
		return nil, err
	}
	buf := make([]byte, 64*1024)
	n, err := uconn.Read(buf)
	if err != nil {
		c.Close()
		return nil, fmt.Errorf("client: read auth reply: %w", err)
	}
	frame, ok := c.reassembler.Feed(buf[:n])
	if !ok || !udpAuthOK(frame) {
		c.Close()
		return nil, ErrUDPAuthFailed
	}
	_ = uconn.SetReadDeadline(time.Time{}) // 清掉期限，交给 ReadLoop

	c.logger.Info("udp authenticated", "addr", addr)
	go c.WritePump()
	go c.ReadLoop()
	return c, nil
}

// udpAuthOK 判断鉴权成功应答。
// 服务端对 MsgHandshakeResp(成功) 编成裸 2 字节 [0x00, 0x01]（frameScheme 编码），
// 用裸字节比对，而不走 scheme.Decode——因为该 2 字节与 TypeHeartbeat 帧头重合。
func udpAuthOK(frame []byte) bool {
	return len(frame) == 2 && frame[0] == 0x00 && frame[1] == 0x01
}

// Send 发送业务消息。
func (c *UDPConn) Send(payload []byte) error {
	if c.isClosed() {
		return ErrUDPConnClosed
	}
	frame, err := scheme.Encode(&comet.Msg{Type: comet.MsgData, Payload: payload})
	if err != nil {
		return err
	}
	select {
	case c.send <- frame:
		return nil
	default:
		return ErrUDPSendFull
	}
}

// OnMessage 注册业务消息回调。可在 DialUDP 之后任意时刻设置。
func (c *UDPConn) OnMessage(fn func([]byte)) {
	c.msgMu.Lock()
	c.onMessage = fn
	c.msgMu.Unlock()
}

// ReadLoop 阻塞读 datagram，重组后分发：心跳回复、业务数据回调。
func (c *UDPConn) ReadLoop() error {
	defer c.Close()
	buf := make([]byte, 64*1024)
	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		default:
		}

		if err := c.conn.SetReadDeadline(time.Now().Add(udpReadTimeout)); err != nil {
			return err
		}
		n, err := c.conn.Read(buf)
		if err != nil {
			return err
		}

		frame, ok := c.reassembler.Feed(buf[:n])
		if !ok {
			continue
		}
		msg, err := scheme.Decode(frame)
		if err != nil {
			c.logger.Warn("decode failed", "err", err)
			continue
		}

		switch msg.Type {
		case comet.MsgHeartbeatReq:
			c.sendHeartbeatReply()
		case comet.MsgData:
			c.msgMu.Lock()
			fn := c.onMessage
			c.msgMu.Unlock()
			if fn != nil {
				// 拷贝，避免回调持有底层 buffer。
				fn(append([]byte(nil), msg.Payload...))
			}
		default:
			c.logger.Warn("unknown message type", "type", msg.Type)
		}
	}
}

// WritePump 从 send 队列取帧，拆包后写 datagram。
func (c *UDPConn) WritePump() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case frame := <-c.send:
			c.msgID++
			for _, dg := range udp.PackFrame(frame, udpMaxSeg, c.msgID) {
				if _, err := c.conn.Write(dg); err != nil {
					c.logger.Warn("write failed", "err", err)
					_ = c.Close()
					return
				}
			}
		}
	}
}

func (c *UDPConn) sendHeartbeatReply() {
	reply, err := scheme.Encode(&comet.Msg{Type: comet.MsgHeartbeatAck})
	if err != nil {
		return
	}
	select {
	case c.send <- reply:
	default:
	}
}

// Close 关闭连接。幂等。
func (c *UDPConn) Close() error {
	c.closedMu.Lock()
	if c.closed {
		c.closedMu.Unlock()
		return nil
	}
	c.closed = true
	c.closedMu.Unlock()
	c.cancel()
	return c.conn.Close()
}

func (c *UDPConn) isClosed() bool {
	c.closedMu.Lock()
	defer c.closedMu.Unlock()
	return c.closed
}
