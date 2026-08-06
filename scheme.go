package comet

import "errors"

// MsgType 语义消息类型（协议无关）：每个 MsgScheme 把自己的线路格式
// 映射到这组语义，Core 的 Dispatch 只认识语义类型。
type MsgType uint8

const (
	MsgNone          MsgType = iota
	MsgHandshakeReq          // 握手/鉴权请求（客户端→服务端）
	MsgHandshakeResp         // 握手/鉴权响应（服务端→客户端）
	MsgHandshakeAck          // 握手确认（客户端→服务端，pomelo 等协议使用）
	MsgHeartbeatReq          // 心跳请求（客户端→服务端）
	MsgHeartbeatAck          // 心跳响应（服务端→客户端）
	MsgData                  // 业务数据（双向）
	MsgKick                  // 踢线（服务端→客户端，pomelo 等协议使用）
)

// Msg 是协议无关的内部消息：语义类型 + 纯字节负载。
// 约定：MsgHandshakeResp 的 Payload[0] 表示握手结果（0=失败，1=成功），
// 各协议方案按自己的线路格式渲染。
type Msg struct {
	Type    MsgType
	Payload []byte
}

// MsgScheme 协议编解码方案：负责 Msg ⇄ 线路字节。
// 默认实现是 frameScheme（原 [2B type][payload] 帧格式）；
// 其他协议（如 pomelo）可实现本接口替换，连接层无需改动。
type MsgScheme interface {
	// Encode 把一条语义消息编码成线路字节（写出）。
	Encode(msg *Msg) ([]byte, error)
	// Decode 从线路字节解析出一条语义消息（读入）。
	Decode(data []byte) (*Msg, error)
}

var (
	ErrUnknownMsgType = errors.New("comet: unknown msg type")
	ErrFrameTooShort  = errors.New("comet: frame too short")
)

// frameScheme 是默认方案：原帧协议 [2B type][payload]。
// 握手/心跳响应是协议特有的固定 2 字节，不带帧头。
type frameScheme struct{}

// NewFrameScheme 创建默认帧方案。
func NewFrameScheme() MsgScheme { return frameScheme{} }

// Decode 实现 MsgScheme：按 2 字节帧头分发。
func (frameScheme) Decode(data []byte) (*Msg, error) {
	if len(data) < FrameHeaderSize {
		return nil, ErrFrameTooShort
	}
	header := [2]byte(data[0:2])
	payload := data[FrameHeaderSize:]
	switch {
	case header == TypeHeartbeat:
		return &Msg{Type: MsgHeartbeatReq, Payload: payload}, nil
	case header == TypeAuth:
		return &Msg{Type: MsgHandshakeReq, Payload: payload}, nil
	case header == TypeMessage:
		return &Msg{Type: MsgData, Payload: payload}, nil
	default:
		return nil, ErrUnknownMsgType
	}
}

// Encode 实现 MsgScheme：语义消息 → 帧字节。
func (frameScheme) Encode(msg *Msg) ([]byte, error) {
	if msg == nil {
		return nil, ErrUnknownMsgType
	}
	switch msg.Type {
	case MsgHeartbeatReq:
		return append(TypeHeartbeat[:], msg.Payload...), nil
	case MsgHeartbeatAck:
		return HeartbeatReply[:], nil
	case MsgHandshakeReq:
		return append(TypeAuth[:], msg.Payload...), nil
	case MsgHandshakeResp:
		if len(msg.Payload) > 0 && msg.Payload[0] == 1 {
			return []byte{0x00, 0x01}, nil // 成功
		}
		return []byte{0x00, 0x00}, nil // 失败
	case MsgData:
		return append(TypeMessage[:], msg.Payload...), nil
	default:
		return nil, ErrUnknownMsgType
	}
}
