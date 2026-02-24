package kktcp

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

type serverConnMgr struct {
	mu    sync.RWMutex
	conns map[kknet.CONN_ID]*tcpConn
}

func newServerConnMgr() *serverConnMgr {
	return &serverConnMgr{
		conns: make(map[kknet.CONN_ID]*tcpConn),
	}
}

var _ kknet.IConnManager = (*serverConnMgr)(nil)

// addConn adds a connection to manager.
func (m *serverConnMgr) addConn(c *tcpConn) {
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
func (m *serverConnMgr) RangeAllConns(fn func(id kknet.CONN_ID, conn kknet.IConn) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, conn := range m.conns {
		if !fn(id, conn) {
			break
		}
	}
}

// GetConn returns a connection by id. Returns nil if the conn does not exist or was removed (e.g. after close).
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
	c := m.GetConn(id)
	if c != nil {
		_ = c.Close()
	}
	m.removeConn(id)
}

// GetCount returns the number of connections.
func (m *serverConnMgr) GetCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}
