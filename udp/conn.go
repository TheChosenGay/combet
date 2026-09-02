// Package udp 提供 UDP 协议的 comet 实现。
//
// 与 ws 不同，UDP 没有 per-connection socket：
//   - 读：由 Server 的单一读循环从共享 PacketConn 收数据，按源地址路由到会话，
//     再经 Conn.HandlePacket 喂给本会话；
//   - 写：每条会话一个 WritePump goroutine，把帧拆包后 WriteTo 回对端。
package udp

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/TheChosenGay/combet"
	"github.com/google/uuid"
)

var (
	ErrSendFull    = errors.New("udp: send buffer full")
	ErrConnClosed  = errors.New("udp: connection closed")
	ErrMsgTooLarge = errors.New("udp: message exceeds MaxMessageSize")
)

const (
	defaultSendBuf    = 256
	defaultInboxBuf   = 512
	defaultReasmTTL   = 10 * time.Second
	defaultMaxSegment = 1400
	defaultMaxMessage = 64 << 10
)

// Config 是 UDP 传输层配置。
type Config struct {
	// MaxSegmentSize 单个 UDP datagram 携带的 payload 上限，默认 1400（兼容 MTU）。
	MaxSegmentSize int
	// MaxMessageSize 重组后单条逻辑消息上限，默认 64KB。
	MaxMessageSize int
	// IdleTimeout 会话空闲回收时长，默认 120s。
	IdleTimeout time.Duration
	// UnauthedTTL 未鉴权（未绑定房间/未握手）会话回收时长，默认 5s。
	UnauthedTTL time.Duration
	// GCInterval 空闲回收扫描周期，默认 5s。
	GCInterval time.Duration
	// SendBuffer 写队列容量，默认 256。
	SendBuffer int
	// InboxBuffer 读队列容量，默认 512。
	InboxBuffer int
	// ReasmTTL 分片在途重组超时，默认 10s。
	ReasmTTL time.Duration
}

func (c Config) withDefaults() Config {
	if c.MaxSegmentSize <= 0 {
		c.MaxSegmentSize = defaultMaxSegment
	}
	if c.MaxMessageSize <= 0 {
		c.MaxMessageSize = defaultMaxMessage
	}
	if c.IdleTimeout <= 0 {
		c.IdleTimeout = 120 * time.Second
	}
	if c.UnauthedTTL <= 0 {
		c.UnauthedTTL = 5 * time.Second
	}
	if c.GCInterval <= 0 {
		c.GCInterval = 5 * time.Second
	}
	if c.SendBuffer <= 0 {
		c.SendBuffer = defaultSendBuf
	}
	if c.InboxBuffer <= 0 {
		c.InboxBuffer = defaultInboxBuf
	}
	if c.ReasmTTL <= 0 {
		c.ReasmTTL = defaultReasmTTL
	}
	return c
}

// Conn 是一条 UDP 逻辑会话，实现 comet.Conn。
//
// 它不是一条网络连接，而是"源地址 ↔ 服务端"之间的一个有状态会话。
// 读侧由 Server 的读循环喂包，写侧由本会话的 WritePump 独立发送。
type Conn struct {
	id   string
	pc   net.PacketConn
	addr net.Addr
	cfg  Config

	inbox chan []byte // 读队列（Server 读循环投递原始 datagram）
	send  chan []byte // 写队列（Write 投递待发帧，WritePump 消费并拆包）

	ctx    context.Context
	cancel context.CancelFunc

	onFrame func([]byte)
	onClose func()

	logger *slog.Logger

	closedMu sync.Mutex
	closed   bool

	lastActive atomic.Int64

	reassembler *Reassembler
	msgID       uint16 // 分片唯一标识，仅 WritePump goroutine 访问
}

// NewConn 创建一条 UDP 会话。pc 为共享 PacketConn，addr 为对端地址。
func NewConn(pc net.PacketConn, addr net.Addr, cfg Config) *Conn {
	cfg = cfg.withDefaults()
	ctx, cancel := context.WithCancel(context.Background())
	id := uuid.NewString()
	return &Conn{
		id:          id,
		pc:          pc,
		addr:        addr,
		cfg:         cfg,
		inbox:       make(chan []byte, cfg.InboxBuffer),
		send:        make(chan []byte, cfg.SendBuffer),
		ctx:         ctx,
		cancel:      cancel,
		logger:      slog.With("component", "udp", "conn_id", id),
		reassembler: NewReassembler(cfg.MaxMessageSize, cfg.MaxSegmentSize, 64),
	}
}

func (c *Conn) ID() string { return c.id }

func (c *Conn) Addr() string { return c.addr.String() }

// SetOnFrame 设置收到完整逻辑帧的回调（Server 接到 Core.Dispatch）。
func (c *Conn) SetOnFrame(fn func([]byte)) { c.onFrame = fn }

// SetOnClose 设置会话关闭时的清理回调（Server 用它移除路由并 Pop ConnManager）。
func (c *Conn) SetOnClose(fn func()) { c.onClose = fn }

// LastActive 返回最近一次收到 datagram 的时间。
func (c *Conn) LastActive() time.Time { return time.Unix(0, c.lastActive.Load()) }

// HandlePacket 由 Server 读循环调用（任意 goroutine），非阻塞投递到本会话读队列。
// 队列满则丢弃（UDP 尽力而为）。
func (c *Conn) HandlePacket(pkt []byte) {
	c.lastActive.Store(time.Now().UnixNano())
	if c.isClosed() {
		return
	}
	select {
	case c.inbox <- pkt:
	default:
	}
}

// Write 投递一帧到写队列。队列满返回 ErrSendFull。
func (c *Conn) Write(_ context.Context, data []byte) error {
	if len(data) > c.cfg.MaxMessageSize {
		return ErrMsgTooLarge
	}
	if c.isClosed() {
		return ErrConnClosed
	}
	select {
	case c.send <- data:
		return nil
	default:
		return ErrSendFull
	}
}

// Start 启动读处理和写协程（由 Server 在登记后调用）。
func (c *Conn) Start() {
	go c.process()
	go c.WritePump()
}

// Close 关闭会话并触发清理回调。幂等。
func (c *Conn) Close() error {
	c.closedMu.Lock()
	if c.closed {
		c.closedMu.Unlock()
		return nil
	}
	c.closed = true
	c.closedMu.Unlock()

	c.cancel()
	if c.onClose != nil {
		c.onClose()
	}
	return nil
}

func (c *Conn) isClosed() bool {
	c.closedMu.Lock()
	defer c.closedMu.Unlock()
	return c.closed
}

// process 消费读队列，重组分片后回调 onFrame。
func (c *Conn) process() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case pkt := <-c.inbox:
			frame, ok := c.reassembler.Feed(pkt)
			if !ok || c.onFrame == nil {
				continue
			}
			c.onFrame(frame)
		}
	}
}

// WritePump 消费写队列，把每帧拆包后 WriteTo 回对端。
func (c *Conn) WritePump() {
	for {
		select {
		case <-c.ctx.Done():
			return
		case frame := <-c.send:
			c.msgID++
			for _, dg := range PackFrame(frame, c.cfg.MaxSegmentSize, c.msgID) {
				if _, err := c.pc.WriteTo(dg, c.addr); err != nil {
					c.logger.Warn("write failed", "err", err)
					_ = c.Close()
					return
				}
			}
		}
	}
}

var _ comet.Conn = (*Conn)(nil)
