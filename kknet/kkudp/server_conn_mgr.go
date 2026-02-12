package kkudp

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
)

type serverConnMgr struct {
	server *Server
	mu     sync.RWMutex
	conns  map[kknet.CONN_ID]*udpConn
	count  int64
}

func newServerConnMgr() *serverConnMgr {
	return &serverConnMgr{
		conns: make(map[kknet.CONN_ID]*udpConn),
		count: 0,
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
	atomic.AddInt64(&m.count, 1)
}

// removeConn removes a connection from manager.
func (m *serverConnMgr) removeConn(id kknet.CONN_ID) {
	m.mu.Lock()
	delete(m.conns, id)
	m.mu.Unlock()
	atomic.AddInt64(&m.count, -1)
}

// GetAllConns returns a snapshot of all connections.
func (m *serverConnMgr) GetAllConns() map[kknet.CONN_ID]kknet.IConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[kknet.CONN_ID]kknet.IConn, len(m.conns))
	for id, c := range m.conns {
		out[id] = c
	}
	return out
}

// RangeAllConns ranges all connections.
func (m *serverConnMgr) RangeAllConns(fn func(id kknet.CONN_ID, conn kknet.IConn)) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, conn := range m.conns {
		fn(id, conn)
	}
}

// GetConn returns a connection by id.
func (m *serverConnMgr) GetConn(id kknet.CONN_ID) kknet.IConn {
	m.mu.RLock()
	c := m.conns[id]
	m.mu.RUnlock()
	if c == nil {
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
func (m *serverConnMgr) GetCount() int64 {
	return atomic.LoadInt64(&m.count)
}
