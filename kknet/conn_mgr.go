package kknet

import (
	"sync"

	"github.com/vvisun/kkdg/utils/xreflect"
)

type ConnInfo struct {
	connID         CONN_ID //连接ID
	userID         USER_ID //用户ID
	sessionID      string  //客户端会话ID
	fromGateNodeId string  //关联网关nodeId
	conn           IConn   //连接对象
}

type ConnManager[T IConn] struct {
	mu    sync.RWMutex
	conns map[CONN_ID]T
	users map[USER_ID]T
}

func NewConnManager[T IConn]() *ConnManager[T] {
	return &ConnManager[T]{
		conns: make(map[CONN_ID]T),
		users: make(map[USER_ID]T),
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
	uid := c.GetUserId()
	if uid != NULL_USER_ID {
		m.users[uid] = c
	}
	m.mu.Unlock()
}

// RemoveConn removes a connection from manager.
func (m *ConnManager[T]) RemoveConn(id CONN_ID) {
	m.mu.Lock()
	c, ok := m.conns[id]
	if !ok {
		m.mu.Unlock()
		return
	}
	uid := c.GetUserId()
	if uid != NULL_USER_ID {
		delete(m.users, uid)
	}
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

// KickUser kicks a connection by user id.
func (m *ConnManager[T]) KickUser(uid USER_ID) {
	c := m.GetConnByUser(uid)
	if c == nil {
		return
	}
	m.KickConn(c.ID())
}

// GetConnByUser returns a connection by user id.
func (m *ConnManager[T]) GetConnByUser(uid USER_ID) IConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.users[uid]
	if !ok {
		return nil
	}
	return c
}

// BindUser binds a connection to a user id.
func (m *ConnManager[T]) BindUser(c IConn, uid USER_ID) {
	if c == nil || uid == NULL_USER_ID {
		return
	}
	m.mu.Lock()
	c.BindUser(uid)
	m.users[uid] = c.(T)
	m.mu.Unlock()
}

// UnbindUser unbinds a connection from a user id.
func (m *ConnManager[T]) UnbindUser(c IConn) {
	if c == nil {
		return
	}
	m.mu.Lock()
	uid := c.GetUserId()
	c.UnbindUser()
	delete(m.users, uid)
	m.mu.Unlock()
}
