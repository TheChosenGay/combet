# combet

长连接接入层核心库（Go）。提供**协议无关**的 comet server 抽象：业务层只面对
`comet.Conn` / `Business` / `MsgScheme`，不感知底层是 WebSocket 还是 UDP，
可插拔地接入 `ws`、`udp` 等传输。适用于游戏/实时消息类网关的长连接接入。

## 特性

- **协议无关核心**：`Core` 只认识语义消息（`MsgType`），通过
  `MsgScheme` 做线格式编解码；默认 `frameScheme`（`[2B type][payload]`），
  换协议（如 pomelo）只需实现 `MsgScheme` 注入。
- **Business / HandshakeHandler**：业务层实现 `Business` 收鉴权与业务消息；
  额外实现 `HandshakeHandler` 即启用"握手阶段"（握手与鉴权分离，pomelo 等可用），
  不注入则保持旧行为（握手即鉴权）。
- **ConnManager**：连接/房间/握手状态管理，线程安全；鉴权成功按
  `userID` 绑定房间，支持广播 `Push`。
- **WebSocket 传输 (`ws`)**：gorilla/websocket，读写分离，读超时可配置
  （心跳/零帧即超时，默认 120s）。
- **UDP 传输 (`udp`)**：
  - 单 socket + 源地址路由到会话（`byAddr`，RWMutex）；会话实现 `comet.Conn`；
  - 每会话一个处理 goroutine（收包+重组+分发）与一个 `WritePump`（拆包发送）；
  - 空闲回收（`IdleTimeout`，默认 120s）+ 未鉴权回收（`UnauthedTTL`，默认 5s）+ 关服显式全关；
  - 大消息分片/重组（`PackFrame` / `Reassembler`），一字节传输头区分完整帧与分片，
    `Core`/`FrameScheme` 无感知；
  - **UDP 客户端 (`client.UDPConn`)**：自动鉴权握手、心跳、分包/重组。
- **应答帧修正**：`frameScheme` 原先将鉴权成功编成 `[0x00,0x01]`，与 `TypeHeartbeat`
  重合、且 `[0x00,0x00]` 不可解码。已为 `MsgHandshakeResp`/`MsgHeartbeatAck`
  分配独立帧头（`TypeHandshakeResp=0x0004`、`TypeHeartbeatAck=0x0005`），
  应答帧可正常 `Encode→Decode` 往返，同时修复了 ws/UDP 客户端鉴权解析。

## 目录结构

```
combet/
├── conn.go           # Conn 接口
├── conn_manager.go   # 连接/房间/握手状态管理
├── protocol.go       # 帧格式/消息类型
├── scheme.go         # MsgScheme 抽象 + 默认 frameScheme
├── business.go       # Business 接口（鉴权/消息回调）
├── handshake.go      # HandshakeHandler（可选握手阶段）
├── server.go         # Core：协议无关分发内核
├── ws/               # WebSocket 传输（Server + Conn）
├── udp/              # UDP 传输（Server + Conn + 分片/重组）
└── client/           # comet 客户端（WebSocket + UDP）
```

## 基准测试

以下为**单机 loopback（127.0.0.1）**下的数据，用于横向对比 ws/udp 的传输开销。
**机器/环境**：Apple M4 Pro · macOS arm64 · `GOMAXPROCS=12` · Go（模块 `go 1.25.3`）。

| 基准（12 并发 / loopback） | 结果 |
|---|---|
| UDP 端到端往返（ping-pong echo） | **~23 µs/op** · 852 B/op · 25 allocs |
| UDP 服务端广播（推送到 16 连接） | **~450 ns/op** · 690 B/op · 9 allocs |
| WebSocket 端到端往返（ping-pong echo） | **~24 µs/op** · 1736 B/op · 19 allocs |
| WebSocket 服务端广播（推送到 16 连接） | **~708 ns/op** · 808 B/op · 9 allocs |

> 说明：loopback 下 ws 与 UDP 往返均约 24µs，瓶颈在 `Core→Business→Write` 路径，
> 而非传输本身。两者真正的差异在**容量**与**差网行为**（见下）。

### 差网 / 丢包行为（应用层 10% datagram 丢失模拟）

| 消息形态 | 存活率 |
|---|---|
| 10KB 大消息（分 ~8 片，`PackFrame`） | **~42%** |
| 30B 小消息（单包） | **~93%** |

> 结论：UDP 分片是"尽力而为"，丢一片即整条失败（≈ 0.9ⁿ，n=分片数）。高频小增量
> （单包 ≤ ~1400B）丢一条不影响下一条；大快照不该依赖"一条 UDP 大消息+分片"。

### 连接容量 / 泄漏

- **10000 条 UDP 会话**全部建立（分批渐增），每批校验 `ConnCount` 递增；
- 全部建立后并发让 10000 条连接**真实收发数据**（send + echo 校验）；关服后
  `goroutine` 回到基线，**无泄漏**。

> 注意：运行 UDP 测试/基准需在可绑定 loopback 的环境（本仓库沙箱默认禁 UDP 绑定，
> 需提权/加网络权限）。`ws` 的基准/泄漏测试为一次性对比即移除，未随库保留；
> 如需复测可按上方条目重建。

## 测试

```bash
go test ./...                 # 全部（默认含两个 10000 连接测试，约多 2~3s）
go test -short ./...          # 跳过 10000 连接测试
go test -race ./...           # 竞态检测（建议配 -short 避免长跑）
go test -run 'TestUDPHighConnectionLeak|TestUDPRampUpTransferLeak' -v ./udp/...
```

覆盖：`frameScheme` 往返、应答帧、Dispatch、握手/旧模式、UDP 分片/重组
（乱序/丢失/GC/超限）、echo/广播/分片回显/未鉴权回收、goroutine 泄漏、丢包模拟、
10000 连接容量+传输。

## 快速使用

```go
// WebSocket
srv := ws.NewServer(":8081", comet.ServerConfig{Business: biz})
srv.ReadTimeout = 120 * time.Second // 默认 120s，可调
go srv.Start(ctx)

// UDP
srv := udp.NewServer(":8081", comet.ServerConfig{Business: biz}, udp.Config{
	MaxSegmentSize: 1400, // 单包 payload 上限，避免 IP 分片
})
go srv.Start(ctx)
```

## 未来计划

- **WebSocket 鉴权失败处理优化**：当前鉴权失败只回失败应答、连接会滞留到读超时
  （默认 120s）。计划在 `BUSINESS` 鉴权失败时主动断开，或给未鉴权连接一个更短的
  超时，避免空连接占 fd（与 UDP 的 `UnauthedTTL` 对齐）。
- **UDP 可靠性层**：为"必须到达"的消息（登录/加入/交易、大快照）增加可靠/有序通道，
  可能采用 **KCP**（ARQ + 低延迟，国内游戏服务器常用）或 QUIC 式实现；与现有
  "不可靠高频状态"通道并存（可靠通道 + 不可靠通道分离）。
- **快照/状态同步策略**：增量 + 定期全量 + 序号（丢弃过期/部分快照，永远用最新完整）；
  结合 AOI/兴趣管理控制单客户端快照体积，优先让每条增量 ≤ 单包（避免大消息分片脆性）。
