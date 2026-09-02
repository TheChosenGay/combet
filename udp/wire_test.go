package udp

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

func TestPackFrameComplete(t *testing.T) {
	frame := []byte{0x00, 0x03, 'h', 'i'}
	dg := PackFrame(frame, 1400, 1)
	if len(dg) != 1 {
		t.Fatalf("want 1 datagram, got %d", len(dg))
	}
	if dg[0][0] != flagComplete {
		t.Fatalf("want complete flag")
	}
	got, ok := NewReassembler(64<<10, 1400, 8).Feed(dg[0])
	if !ok || !bytes.Equal(got, frame) {
		t.Fatalf("roundtrip mismatch: %v", got)
	}
}

func TestPackFrameFragmentInOrder(t *testing.T) {
	frame := bytes.Repeat([]byte{0xaa}, 4000)
	dg := PackFrame(frame, 1400, 42)
	if len(dg) < 2 {
		t.Fatalf("want fragmented, got %d dgrams", len(dg))
	}
	r := NewReassembler(64<<10, 1400, 8)
	var got []byte
	for _, d := range dg {
		if f, ok := r.Feed(d); ok {
			got = f
		}
	}
	if got == nil || !bytes.Equal(got, frame) {
		t.Fatalf("reassembly mismatch: len=%d", len(got))
	}
}

func TestPackFrameFragmentOutOfOrder(t *testing.T) {
	frame := bytes.Repeat([]byte{0xbb}, 3000)
	dg := PackFrame(frame, 1400, 7)
	r := NewReassembler(64<<10, 1400, 8)
	var got []byte
	for i := len(dg) - 1; i >= 0; i-- {
		if f, ok := r.Feed(dg[i]); ok {
			got = f
		}
	}
	if got == nil || !bytes.Equal(got, frame) {
		t.Fatalf("out-of-order reassembly mismatch: len=%d", len(got))
	}
}

func TestReassemblerLossNoComplete(t *testing.T) {
	frame := bytes.Repeat([]byte{0xcc}, 3000)
	dg := PackFrame(frame, 1400, 9)
	r := NewReassembler(64<<10, 1400, 8)
	for _, d := range dg[:len(dg)-1] {
		if _, ok := r.Feed(d); ok {
			t.Fatal("should not complete on loss")
		}
	}
	if len(r.pending) != 1 {
		t.Fatalf("pending = %d, want 1", len(r.pending))
	}
}

func TestReassemblerGC(t *testing.T) {
	frame := bytes.Repeat([]byte{0xdd}, 3000)
	dg := PackFrame(frame, 1400, 11)
	r := NewReassembler(64<<10, 1400, 8)
	r.Feed(dg[0])
	if len(r.pending) != 1 {
		t.Fatalf("pending = %d", len(r.pending))
	}
	r.GC(time.Now().Add(20*time.Second), 10*time.Second)
	if len(r.pending) != 0 {
		t.Fatalf("pending after GC = %d", len(r.pending))
	}
}

func TestReassemblerRejectsTooManySegments(t *testing.T) {
	r := NewReassembler(64<<10, 1400, 8)
	// 构造一个 total 远超 maxTotal 的分片
	dg := make([]byte, 1+fragHeader+1)
	dg[0] = flagFragment
	binary.BigEndian.PutUint16(dg[1:3], 1)
	dg[3] = 0
	dg[4] = 200
	if _, ok := r.Feed(dg); ok {
		t.Fatal("should reject fragment with total > maxTotal")
	}
}

func TestReassemblerRejectsUnknownFlag(t *testing.T) {
	r := NewReassembler(64<<10, 1400, 8)
	if _, ok := r.Feed([]byte{0x02, 0x01}); ok {
		t.Fatal("should reject unknown flag")
	}
}
