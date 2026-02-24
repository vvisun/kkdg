package kktcptls

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

type serverConnMgr struct {
	mu    sync.RWMutex
	conns map[kknet.CONN_ID]*tlsConn
}

func newServerConnMgr() *serverConnMgr {
	return &serverConnMgr{
		conns: make(map[kknet.CONN_ID]*tlsConn),
	}
}

var _ kknet.IConnManager = (*serverConnMgr)(nil)

func (m *serverConnMgr) addConn(c *tlsConn) {
	if c == nil {
		return
	}
	m.mu.Lock()
	m.conns[c.ID()] = c
	m.mu.Unlock()
}

func (m *serverConnMgr) removeConn(id kknet.CONN_ID) {
	m.mu.Lock()
	delete(m.conns, id)
	m.mu.Unlock()
}

func (m *serverConnMgr) GetAllConns() map[kknet.CONN_ID]kknet.IConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make(map[kknet.CONN_ID]kknet.IConn, len(m.conns))
	for id, c := range m.conns {
		out[id] = c
	}
	return out
}

func (m *serverConnMgr) RangeAllConns(fn func(id kknet.CONN_ID, conn kknet.IConn) bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for id, conn := range m.conns {
		if !fn(id, conn) {
			break
		}
	}
}

func (m *serverConnMgr) GetConn(id kknet.CONN_ID) kknet.IConn {
	m.mu.RLock()
	c := m.conns[id]
	m.mu.RUnlock()
	if c == nil {
		return nil
	}
	return c
}

func (m *serverConnMgr) KickConn(id kknet.CONN_ID) {
	c := m.GetConn(id)
	if c != nil {
		_ = c.Close()
	}
	m.removeConn(id)
}

func (m *serverConnMgr) GetCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.conns)
}
