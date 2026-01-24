package kkws

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

type serverConnMgr struct {
	mu    sync.RWMutex
	conns map[int64]*wsConn
}

func newServerConnMgr() *serverConnMgr {
	return &serverConnMgr{
		conns: make(map[int64]*wsConn),
	}
}

var _ kknet.IConnManager = (*serverConnMgr)(nil)

// addConn adds a connection to manager.
func (m *serverConnMgr) addConn(c *wsConn) {
	if c == nil {
		return
	}
	m.mu.Lock()
	m.conns[c.ID()] = c
	m.mu.Unlock()
}

// removeConn removes a connection from manager.
func (m *serverConnMgr) removeConn(id int64) {
	m.mu.Lock()
	delete(m.conns, id)
	m.mu.Unlock()
}

// GetAllConns returns a snapshot of all connections.
func (m *serverConnMgr) GetAllConns() map[int64]kknet.IConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[int64]kknet.IConn, len(m.conns))
	for id, c := range m.conns {
		out[id] = c
	}
	return out
}

// GetConn returns a connection by id.
func (m *serverConnMgr) GetConn(id int64) kknet.IConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.conns[id]
}

// KickConn closes and removes a connection.
func (m *serverConnMgr) KickConn(id int64) {
	c := m.GetConn(id)
	if c != nil {
		_ = c.Close()
	}
	m.removeConn(id)
}
