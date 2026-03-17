package kknet

import (
	"sync"

	"github.com/vvisun/kkdg/utils/xreflect"
)

// ConnManager is a manager for connections.
// 连接管理器。管理连接的添加、移除、获取、踢出、获取数量、遍历所有连接。
type ConnManager[T IConn] struct {
	mu    sync.RWMutex
	conns map[CONN_ID]T
}

func NewConnManager[T IConn]() *ConnManager[T] {
	return &ConnManager[T]{
		conns: make(map[CONN_ID]T),
	}
}

// 实现IConnManager接口。
var _ IConnManager = (*ConnManager[IConn])(nil)

// AddConn adds a connection to manager.
// 添加连接。将连接添加到管理器中。
func (m *ConnManager[T]) AddConn(c T) {
	if xreflect.IsNil(c) {
		return
	}
	m.mu.Lock()
	m.conns[c.ID()] = c
	m.mu.Unlock()
}

// RemoveConn removes a connection from manager.
// 移除连接。从管理器中移除连接。
func (m *ConnManager[T]) RemoveConn(id CONN_ID) {
	m.mu.Lock()
	delete(m.conns, id)
	m.mu.Unlock()
}

// GetConn returns a connection by id.
// 获取连接。
func (m *ConnManager[T]) GetConn(id CONN_ID) IConn {
	m.mu.RLock()
	c, ok := m.conns[id]
	m.mu.RUnlock()
	if !ok {
		return nil
	}
	return c
}

// KickConn closes and removes a connection.
// 踢出连接。关闭连接并从管理器中移除。
func (m *ConnManager[T]) KickConn(id CONN_ID) {
	c := m.GetConn(id)
	if c != nil {
		_ = c.Close()
	}
	m.RemoveConn(id)
}

// GetCount returns the number of connections.
// 获取连接数量。
func (m *ConnManager[T]) GetCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}

// RangeAllConns ranges all connections. when fn returns false, the iteration will be stopped.
// 遍历所有连接，当fn返回false时，遍历停止。
func (m *ConnManager[T]) RangeAllConns(fn func(id CONN_ID, conn IConn) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, conn := range m.conns {
		if !fn(id, conn) {
			break
		}
	}
}
