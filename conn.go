// Package comet 提供长连接接入层的核心抽象。
// 业务层不直接感知 WebSocket/TCP，只面对 Conn 接口。
package comet

import "context"

// Conn 代表一条客户端长连接。
// 每条连接有唯一 ID、远端地址，可通过 Write 向客户端发送数据。
type Conn interface {
	ID() string
	Addr() string
	Write(ctx context.Context, data []byte) error
}
