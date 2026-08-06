package comet

import "context"

// HandshakeHandler 处理握手阶段的协商（pomelo 等"握手纯净 + 之后鉴权"的协议）。
//
// Business 实现者可以同时实现本接口来启用握手阶段（由 Core 自动识别）：
//   - MsgHandshakeReq 走 OnHandshake（不鉴权、不绑定身份）；
//   - 客户端发 MsgHandshakeAck 后业务消息才放行；
//   - 鉴权由业务层在后续数据消息（如 login 路由）中完成，成功后可用
//     ConnManager.Bind 绑定身份（供后续消息携带 userID）。
//
// Business 未实现本接口则保持旧行为：MsgHandshakeReq 即鉴权（OnAuth + Bind）。
type HandshakeHandler interface {
	// OnHandshake 处理握手请求（payload 为客户端握手负载），返回握手响应负载
	// （协议自定义，如 pomelo 的 JSON {"code":200,"sys":{...}}）。
	OnHandshake(ctx context.Context, conn Conn, payload []byte) ([]byte, error)
}
