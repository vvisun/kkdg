package kktcptls

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

type serverConnMgr struct {
	mu    sync.RWMutex
	conns map[kknet.CONN_ID]*tlsConn
	users map[kknet.USER_ID]*tlsConn
}

func newServerConnMgr() *serverConnMgr {
	return &serverConnMgr{
		conns: make(map[kknet.CONN_ID]*tlsConn),
		users: make(map[kknet.USER_ID]*tlsConn),
	}
}

var _ kknet.IConnManager = (*serverConnMgr)(nil)

func (m *serverConnMgr) addConn(c *tlsConn) {
	if c == nil {
		return
	}
	m.mu.Lock()
	m.conns[c.ID()] = c
	uid := c.GetUserId()
	if uid != kknet.NULL_USER_ID {
		m.users[uid] = c
	}
	m.mu.Unlock()
}

func (m *serverConnMgr) removeConn(id kknet.CONN_ID) {
	m.mu.Lock()
	c, ok := m.conns[id]
	if !ok || c == nil {
		m.mu.Unlock()
		return
	}
	uid := c.GetUserId()
	if uid != kknet.NULL_USER_ID {
		delete(m.users, uid)
	}
	delete(m.conns, id)
	m.mu.Unlock()
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
	c, ok := m.conns[id]
	m.mu.RUnlock()
	if !ok || c == nil {
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

// KickUser kicks a connection by user id.
func (m *serverConnMgr) KickUser(uid kknet.USER_ID) {
	c := m.GetConnByUser(uid)
	if c == nil {
		return
	}
	m.KickConn(c.ID())
}

// GetConnByUser returns a connection by user id.
func (m *serverConnMgr) GetConnByUser(uid kknet.USER_ID) kknet.IConn {
	m.mu.RLock()
	defer m.mu.RUnlock()
	c, ok := m.users[uid]
	if !ok || c == nil {
		return nil
	}
	return c
}

// BindUser binds a connection to a user id.
func (m *serverConnMgr) BindUser(c kknet.IConn, uid kknet.USER_ID) {
	if c == nil || uid == kknet.NULL_USER_ID {
		return
	}
	m.mu.Lock()
	c.BindUser(uid)
	m.users[uid] = c.(*tlsConn)
	m.mu.Unlock()
}

// UnbindUser unbinds a connection from a user id.
func (m *serverConnMgr) UnbindUser(c kknet.IConn) {
	if c == nil {
		return
	}
	m.mu.Lock()
	uid := c.GetUserId()
	c.UnbindUser()
	delete(m.users, uid)
	m.mu.Unlock()
}
