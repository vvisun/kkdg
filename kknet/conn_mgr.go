package kknet

import (
	"sync"

	"github.com/vvisun/kkdg/utils/xreflect"
)

type ConnManager[T IConn] struct {
	mu    sync.RWMutex
	conns map[CONN_ID]T
}

func NewConnManager[T IConn]() *ConnManager[T] {
	return &ConnManager[T]{
		conns: make(map[CONN_ID]T),
	}
}

var _ IConnManager = (*ConnManager[IConn])(nil)

// AddConn adds a connection to manager.
func (m *ConnManager[T]) AddConn(c T) {
	if xreflect.IsNil(c) {
		return
	}
	m.mu.Lock()
	m.conns[c.ID()] = c
	m.mu.Unlock()
}

// RemoveConn removes a connection from manager.
func (m *ConnManager[T]) RemoveConn(id CONN_ID) {
	m.mu.Lock()
	delete(m.conns, id)
	m.mu.Unlock()
}

// RangeAllConns ranges all connections.
func (m *ConnManager[T]) RangeAllConns(fn func(id CONN_ID, conn IConn) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, conn := range m.conns {
		if !fn(id, conn) {
			break
		}
	}
}

// GetConn returns a connection by id.
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
func (m *ConnManager[T]) KickConn(id CONN_ID) {
	c := m.GetConn(id)
	if c != nil {
		_ = c.Close()
	}
	m.RemoveConn(id)
}

// GetCount returns the number of connections.
func (m *ConnManager[T]) GetCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}
