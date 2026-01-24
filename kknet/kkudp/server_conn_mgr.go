package kkudp

import (
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

type serverConnMgr struct {
	server *Server
	mu     sync.RWMutex
	conns  map[int64]*udpConn
}

func newServerConnMgr() *serverConnMgr {
	return &serverConnMgr{
		conns: make(map[int64]*udpConn),
	}
}

var _ kknet.IConnManager = (*serverConnMgr)(nil)

// addConn adds a connection to manager.
func (m *serverConnMgr) addConn(c *udpConn) {
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
	if m.server == nil {
		c := m.GetConn(id)
		if c != nil {
			_ = c.Close()
		}
		m.removeConn(id)
		return
	}
	conn := m.server.removeConnByID(id)
	if conn != nil {
		m.server.closeConn(conn, kkerrors.ErrServerStopped)
		return
	}
	m.removeConn(id)
}
