package comet

import "context"

// Business 是 comet 向业务层暴露的协议接口。
// 业务层（services/comet）实现此接口，注入到 Server。
type Business interface {
	// OnAuth 鉴权回调。payload 是客户端发来的鉴权消息体（不含帧头）。
	// 返回 userID 表示鉴权成功；返回空字符串 + error 表示失败。
	OnAuth(ctx context.Context, payload []byte) (userID string, err error)

	// OnMessage 业务消息回调。payload 是客户端发来的消息体（不含帧头）。
	// connID 和 userID 标识消息来源。
	OnMessage(ctx context.Context, connID string, userID string, payload []byte) error
}
