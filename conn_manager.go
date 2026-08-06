package comet

import (
	"context"
	"sync"
)

// ConnManager 管理所有连接及其房间绑定，线程安全。
type ConnManager struct {
	mu       sync.RWMutex
	conns    map[string]Conn            // connID → Conn
	rooms    map[string]map[string]Conn // roomID → connID → Conn
	connRoom map[string]string          // connID → roomID
}

// NewConnManager 创建 ConnManager。
func NewConnManager() *ConnManager {
	return &ConnManager{
		conns:    make(map[string]Conn),
		rooms:    make(map[string]map[string]Conn),
		connRoom: make(map[string]string),
	}
}

// Push 注册一条新连接。
func (m *ConnManager) Push(c Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.conns[c.ID()] = c
}

// Pop 移除一条连接，同时清理其房间绑定。
func (m *ConnManager) Pop(c Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	delete(m.conns, c.ID())

	roomID := m.connRoom[c.ID()]
	if roomID == "" {
		return
	}
	delete(m.connRoom, c.ID())

	if members, ok := m.rooms[roomID]; ok {
		delete(members, c.ID())
		if len(members) == 0 {
			delete(m.rooms, roomID)
		}
	}
}

// Bind 将连接绑定到指定房间（OnAuth 成功后调用）。
func (m *ConnManager) Bind(roomID string, c Conn) {
	m.mu.Lock()
	defer m.mu.Unlock()

	members, ok := m.rooms[roomID]
	if !ok {
		members = make(map[string]Conn)
		m.rooms[roomID] = members
	}
	members[c.ID()] = c
	m.connRoom[c.ID()] = roomID
}

// RoomOf 返回连接所属的房间 ID。未绑定时返回空字符串。
func (m *ConnManager) RoomOf(connID string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.connRoom[connID]
}

// PushToRoom 向指定房间的所有连接广播消息。
func (m *ConnManager) PushToRoom(roomID string, data []byte) int {
	m.mu.RLock()
	members, ok := m.rooms[roomID]
	conns := make([]Conn, 0, len(members))
	if ok {
		for _, c := range members {
			conns = append(conns, c)
		}
	}
	m.mu.RUnlock()

	if len(conns) == 0 {
		return 0
	}

	delivered := 0
	for _, c := range conns {
		if err := c.Write(context.Background(), data); err == nil {
			delivered++
		}
	}
	return delivered
}

// ConnCount 返回当前连接总数。
func (m *ConnManager) ConnCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

// RoomOnline 返回指定房间的在线连接数和是否在线。
func (m *ConnManager) RoomOnline(roomID string) (online bool, count int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	members, ok := m.rooms[roomID]
	if !ok {
		return false, 0
	}
	count = len(members)
	return count > 0, count
}

// RoomCount 返回当前房间总数。
func (m *ConnManager) RoomCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.rooms)
}
