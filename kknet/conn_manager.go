package kknet

import "sync"

// ConnManager manages server connections.
type ConnManager struct {
	mu    sync.RWMutex
	conns map[int64]IConn
}

// NewConnManager creates a new connection manager.
func NewConnManager() *ConnManager {
	return &ConnManager{
		conns: make(map[int64]IConn),
	}
}

// AddConn adds a connection to manager.
func (m *ConnManager) AddConn(c IConn) {
	if c == nil {
		return
	}
	m.mu.Lock()
	m.conns[c.ID()] = c
	m.mu.Unlock()
}

// RemoveConn removes a connection from manager.
func (m *ConnManager) RemoveConn(id int64) {
	m.mu.Lock()
	delete(m.conns, id)
	m.mu.Unlock()
}

// GetAllConns returns a snapshot of all connections.
func (m *ConnManager) GetAllConns() map[int64]IConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[int64]IConn, len(m.conns))
	for id, c := range m.conns {
		out[id] = c
	}
	return out
}

// GetConn returns a connection by id.
func (m *ConnManager) GetConn(id int64) IConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[id]
}

// KickConn closes and removes a connection.
func (m *ConnManager) KickConn(id int64) {
	c := m.GetConn(id)
	if c != nil {
		_ = c.Close()
	}
	m.RemoveConn(id)
}
