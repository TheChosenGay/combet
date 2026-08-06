package comet

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

// testScheme 是极简测试方案：线路字节 = 1B 语义类型 + payload。
// 用于隔离"握手阶段"逻辑与具体线路格式。
type testScheme struct{}

func (testScheme) Encode(msg *Msg) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("nil msg")
	}
	return append([]byte{byte(msg.Type)}, msg.Payload...), nil
}

func (testScheme) Decode(data []byte) (*Msg, error) {
	if len(data) == 0 {
		return nil, errors.New("empty")
	}
	return &Msg{Type: MsgType(data[0]), Payload: data[1:]}, nil
}

type fakeHandshake struct {
	reply []byte
	calls int
}

func (h *fakeHandshake) OnHandshake(_ context.Context, _ Conn, _ []byte) ([]byte, error) {
	h.calls++
	return h.reply, nil
}

type recordingBusiness struct {
	msgs []string // "userID:payload"
}

func (b *recordingBusiness) OnAuth(_ context.Context, _ []byte) (string, error) {
	return "u1", nil
}

func (b *recordingBusiness) OnMessage(_ context.Context, _, userID string, payload []byte) error {
	b.msgs = append(b.msgs, userID+":"+string(payload))
	return nil
}

// TestHandshakeMode：握手 → ack → 业务消息放行；握手前数据被丢弃。
func TestHandshakeMode(t *testing.T) {
	biz := &recordingBusiness{}
	core := NewCore(ServerConfig{
		Business:  biz,
		Scheme:    testScheme{},
		Handshake: &fakeHandshake{reply: []byte(`{"code":200}`)},
	})
	conn := &fakeConn{}

	// 1. 握手前发业务消息 → 丢弃
	core.Dispatch(context.Background(), conn, []byte{byte(MsgData), 'a'})
	if len(biz.msgs) != 0 {
		t.Fatalf("data before handshake not dropped: %v", biz.msgs)
	}

	// 2. 握手请求 → 回握手响应
	core.Dispatch(context.Background(), conn, []byte{byte(MsgHandshakeReq), 'x'})
	if len(conn.written) != 1 ||
		!bytes.Equal(conn.written[0], []byte{byte(MsgHandshakeResp), '{', '"', 'c', 'o', 'd', 'e', '"', ':', '2', '0', '0', '}'}) {
		t.Fatalf("handshake reply = %v", conn.written)
	}

	// 3. ack 后业务消息放行（未登录时 userID 为空，交给业务层）
	core.Dispatch(context.Background(), conn, []byte{byte(MsgHandshakeAck)})
	core.Dispatch(context.Background(), conn, []byte{byte(MsgData), 'b'})
	if len(biz.msgs) != 1 || biz.msgs[0] != ":b" {
		t.Fatalf("msgs = %v", biz.msgs)
	}
}

// TestLegacyMode：不注入 Handshake 时，未鉴权的业务消息被拒绝。
func TestLegacyModeUnauthenticatedRejected(t *testing.T) {
	biz := &recordingBusiness{}
	core := NewCore(ServerConfig{Business: biz, Scheme: testScheme{}})
	conn := &fakeConn{}

	core.Dispatch(context.Background(), conn, []byte{byte(MsgData), 'a'})
	if len(biz.msgs) != 0 {
		t.Fatalf("unauthenticated data not rejected: %v", biz.msgs)
	}

	// 鉴权（握手即鉴权）成功后业务消息放行
	core.Dispatch(context.Background(), conn, []byte{byte(MsgHandshakeReq), 't'})
	core.Dispatch(context.Background(), conn, []byte{byte(MsgData), 'b'})
	if len(biz.msgs) != 1 || biz.msgs[0] != "u1:b" {
		t.Fatalf("msgs = %v", biz.msgs)
	}
}

// TestHandshakeStateCleanedOnPop：连接断开后握手状态被清理。
func TestHandshakeStateCleanedOnPop(t *testing.T) {
	core := NewCore(ServerConfig{
		Business:  &recordingBusiness{},
		Scheme:    testScheme{},
		Handshake: &fakeHandshake{reply: []byte("ok")},
	})
	conn := &fakeConn{}
	core.Dispatch(context.Background(), conn, []byte{byte(MsgHandshakeAck)})
	if !core.cfg.ConnManager.IsHandshaken(conn.ID()) {
		t.Fatal("not handshaken")
	}
	core.cfg.ConnManager.Pop(conn)
	if core.cfg.ConnManager.IsHandshaken(conn.ID()) {
		t.Fatal("handshake state not cleaned")
	}
}
