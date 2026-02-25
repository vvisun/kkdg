package kkudp

import (
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

type serverConnMgr struct {
	server *Server
	mu     sync.RWMutex
	conns  map[kknet.CONN_ID]*udpConn
}

func newServerConnMgr() *serverConnMgr {
	return &serverConnMgr{
		conns: make(map[kknet.CONN_ID]*udpConn),
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
func (m *serverConnMgr) removeConn(id kknet.CONN_ID) {
	m.mu.Lock()
	delete(m.conns, id)
	m.mu.Unlock()
}

// RangeAllConns ranges all connections.
func (m *serverConnMgr) RangeAllConns(fn func(id kknet.CONN_ID, conn kknet.IConn) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, conn := range m.conns {
		if !fn(id, conn) {
			break
		}
	}
}

// GetConn returns a connection by id.
func (m *serverConnMgr) GetConn(id kknet.CONN_ID) kknet.IConn {
	m.mu.RLock()
	c, ok := m.conns[id]
	m.mu.RUnlock()
	if !ok || c == nil {
		return nil
	}
	return c
}

// KickConn closes and removes a connection.
func (m *serverConnMgr) KickConn(id kknet.CONN_ID) {
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

// GetCount returns the number of connections.
func (m *serverConnMgr) GetCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}
