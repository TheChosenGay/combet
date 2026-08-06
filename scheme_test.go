package comet

import (
	"bytes"
	"context"
	"testing"
)

func TestFrameSchemeRoundTrip(t *testing.T) {
	s := NewFrameScheme()
	cases := []*Msg{
		{Type: MsgHeartbeatReq, Payload: nil},
		{Type: MsgHandshakeReq, Payload: []byte("token")},
		{Type: MsgData, Payload: []byte("hello")},
	}
	for _, m := range cases {
		wire, err := s.Encode(m)
		if err != nil {
			t.Fatalf("encode %+v: %v", m, err)
		}
		got, err := s.Decode(wire)
		if err != nil {
			t.Fatalf("decode %+v: %v", m, err)
		}
		if got.Type != m.Type || !bytes.Equal(got.Payload, m.Payload) {
			t.Fatalf("roundtrip mismatch: want %+v, got %+v", m, got)
		}
	}
}

func TestFrameSchemeReplies(t *testing.T) {
	s := NewFrameScheme()
	hb, err := s.Encode(&Msg{Type: MsgHeartbeatAck})
	if err != nil || !bytes.Equal(hb, []byte{0x00, 0x00}) {
		t.Fatalf("heartbeat ack = %v, %v", hb, err)
	}
	ok, err := s.Encode(&Msg{Type: MsgHandshakeResp, Payload: []byte{0x01}})
	if err != nil || !bytes.Equal(ok, []byte{0x00, 0x01}) {
		t.Fatalf("auth ok = %v, %v", ok, err)
	}
	fail, err := s.Encode(&Msg{Type: MsgHandshakeResp, Payload: []byte{0x00}})
	if err != nil || !bytes.Equal(fail, []byte{0x00, 0x00}) {
		t.Fatalf("auth fail = %v, %v", fail, err)
	}
}

func TestFrameSchemeInvalid(t *testing.T) {
	s := NewFrameScheme()
	if _, err := s.Decode([]byte{0x00}); err != ErrFrameTooShort {
		t.Fatalf("short frame err = %v", err)
	}
	if _, err := s.Decode([]byte{0x00, 0x09, 0x01}); err != ErrUnknownMsgType {
		t.Fatalf("unknown type err = %v", err)
	}
}

// ---- Dispatch 协议无关性验证 ----

type fakeConn struct {
	written [][]byte
}

func (f *fakeConn) ID() string   { return "conn-1" }
func (f *fakeConn) Addr() string { return "127.0.0.1" }
func (f *fakeConn) Write(_ context.Context, data []byte) error {
	f.written = append(f.written, data)
	return nil
}

type fakeBusiness struct {
	authOK bool
}

func (b *fakeBusiness) OnAuth(_ context.Context, _ []byte) (string, error) {
	if b.authOK {
		return "user-1", nil
	}
	return "", nil
}

func (b *fakeBusiness) OnMessage(_ context.Context, _, _ string, _ []byte) error {
	return nil
}

func TestDispatchHeartbeat(t *testing.T) {
	core := NewCore(ServerConfig{Business: &fakeBusiness{}})
	conn := &fakeConn{}
	core.Dispatch(context.Background(), conn, []byte{0x00, 0x01})
	if len(conn.written) != 1 || !bytes.Equal(conn.written[0], []byte{0x00, 0x00}) {
		t.Fatalf("heartbeat reply = %v", conn.written)
	}
}

func TestDispatchAuth(t *testing.T) {
	core := NewCore(ServerConfig{Business: &fakeBusiness{authOK: true}})
	conn := &fakeConn{}
	core.Dispatch(context.Background(), conn, append([]byte{0x00, 0x02}, "token"...))
	if len(conn.written) != 1 || !bytes.Equal(conn.written[0], []byte{0x00, 0x01}) {
		t.Fatalf("auth reply = %v", conn.written)
	}
}
