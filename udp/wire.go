package udp

import (
	"encoding/binary"
	"sync"
	"time"
)

// UDP datagram 传输头，位于每次 ReadFrom/WriteTo 的数据最前面。
//
//	完整帧:   [0x00][ frame bytes ... ]
//	分片:     [0x01][2B msgID][1B seq][1B total][ 该片 frame 片段 ]
//
// 完整帧直接把 frame 原样附在后面；分片则带重组头，方便接收端按 msgID 攒齐。
// Core/FrameScheme 完全感知不到这层——重组完后才交给 Dispatch。
const (
	flagComplete = uint8(0x00)
	flagFragment = uint8(0x01)
	fragHeader   = 4 // 2B msgID + 1B seq + 1B total
)

// PackFrame 把一个逻辑帧打包成 1..N 个 UDP datagram。
// 单包能装下时返回一个 [flagComplete][frame]；否则按 maxSeg 切成多个分片。
// msgID 用于标识同一条逻辑消息，调用方需保证不同消息的 msgID 不重复。
func PackFrame(frame []byte, maxSeg int, msgID uint16) [][]byte {
	if len(frame) <= maxSeg {
		out := make([]byte, 1+len(frame))
		out[0] = flagComplete
		copy(out[1:], frame)
		return [][]byte{out}
	}

	total := (len(frame) + maxSeg - 1) / maxSeg
	if total > 255 {
		// 超出单片上限：降级为单包（可靠层应在此前限制消息大小，这里兜底）
		out := make([]byte, 1+len(frame))
		out[0] = flagComplete
		copy(out[1:], frame)
		return [][]byte{out}
	}

	dgrams := make([][]byte, 0, total)
	for i := 0; i < total; i++ {
		start := i * maxSeg
		end := start + maxSeg
		if end > len(frame) {
			end = len(frame)
		}
		chunk := frame[start:end]

		dg := make([]byte, 1+fragHeader+len(chunk))
		dg[0] = flagFragment
		binary.BigEndian.PutUint16(dg[1:3], msgID)
		dg[3] = byte(i)
		dg[4] = byte(total)
		copy(dg[1+fragHeader:], chunk)
		dgrams = append(dgrams, dg)
	}
	return dgrams
}

// pending 是一条正在重组中的分片消息。
type pending struct {
	total  int
	count  int
	chunks [][]byte
	lastAt time.Time
}

// Reassembler 把分片 datagram 重组成完整逻辑帧。
// 通常被单个 goroutine 消费（server 会话的 process、client 的 ReadLoop），
// 但 GC 可能来自其他 goroutine，所以内部用 mutex 保护。
type Reassembler struct {
	mu          sync.Mutex
	pending     map[uint16]*pending
	maxTotal    int // 单条消息最多分片数
	maxMsgLen   int // 重组后最大帧长度
	maxInflight int // 同时在途的最大分片消息数
}

// NewReassembler 创建重组器。maxMsgLen 为重组后最大帧长，maxSeg 为单包片段大小，
// maxInflight 限制同时在途的分片消息数。
func NewReassembler(maxMsgLen, maxSeg, maxInflight int) *Reassembler {
	if maxMsgLen <= 0 {
		maxMsgLen = 64 << 10
	}
	if maxSeg <= 0 {
		maxSeg = 1400
	}
	if maxInflight <= 0 {
		maxInflight = 32
	}
	maxTotal := (maxMsgLen + maxSeg - 1) / maxSeg
	if maxTotal > 255 {
		maxTotal = 255
	}
	return &Reassembler{
		pending:     make(map[uint16]*pending),
		maxTotal:    maxTotal,
		maxMsgLen:   maxMsgLen,
		maxInflight: maxInflight,
	}
}

// Feed 喂入一个 datagram；若形成完整帧则返回 (frame, true)。
// 对完整帧直接返回其载荷；对分片则累积，凑齐后返回重组帧。
// 非法/超限/满员的分片会被丢弃（返回 nil, false）。
func (r *Reassembler) Feed(dgram []byte) ([]byte, bool) {
	if len(dgram) < 1 {
		return nil, false
	}
	if dgram[0] == flagComplete {
		return dgram[1:], true
	}
	if dgram[0] != flagFragment || len(dgram) < 1+fragHeader {
		return nil, false
	}

	msgID := binary.BigEndian.Uint16(dgram[1:3])
	seq := int(dgram[3])
	total := int(dgram[4])
	payload := dgram[1+fragHeader:]

	if total < 1 || total > r.maxTotal || seq < 0 || seq >= total {
		return nil, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	p, ok := r.pending[msgID]
	if !ok {
		if len(r.pending) >= r.maxInflight {
			return nil, false
		}
		p = &pending{total: total, chunks: make([][]byte, total), lastAt: time.Now()}
		r.pending[msgID] = p
	} else if p.total != total {
		return nil, false
	}

	if p.chunks[seq] == nil {
		// 拷贝，避免持有读缓冲区的引用（读缓冲区可能被复用）
		seg := make([]byte, len(payload))
		copy(seg, payload)
		p.chunks[seq] = seg
		p.count++
		p.lastAt = time.Now()
	}

	if p.count != p.total {
		return nil, false
	}

	// 组装
	size := 0
	for _, c := range p.chunks {
		size += len(c)
	}
	if size > r.maxMsgLen {
		delete(r.pending, msgID)
		return nil, false
	}
	frame := make([]byte, 0, size)
	for _, c := range p.chunks {
		frame = append(frame, c...)
	}
	delete(r.pending, msgID)
	return frame, true
}

// GC 清理超过 ttl 未完成的分片消息，避免在途内存泄漏。
func (r *Reassembler) GC(now time.Time, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, p := range r.pending {
		if now.Sub(p.lastAt) > ttl {
			delete(r.pending, id)
		}
	}
}
