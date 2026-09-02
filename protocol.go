package comet

// 帧格式：[2 bytes type][N bytes payload]
//
// 连接建立后，第一条消息必须是 Auth。
// 后续消息可以是 Heartbeat 或 Message。

const (
	// FrameHeaderSize 帧头长度（2 字节类型）
	FrameHeaderSize = 2
)

// 消息类型 — 2 字节帧头
var (
	TypeHeartbeat     = [2]byte{0x00, 0x01} // 心跳请求（客户端 → 服务端）
	TypeAuth          = [2]byte{0x00, 0x02} // 鉴权/握手请求（客户端 → 服务端）
	TypeMessage       = [2]byte{0x00, 0x03} // 业务数据（双向）
	TypeHandshakeResp = [2]byte{0x00, 0x04} // 鉴权/握手响应（服务端 → 客户端）
	TypeHeartbeatAck  = [2]byte{0x00, 0x05} // 心跳响应（服务端 → 客户端）
)

// HeartbeatReply 是旧协议的固定心跳应答字节（已废弃，保留仅为兼容）。
var HeartbeatReply = [2]byte{0x00, 0x00}
