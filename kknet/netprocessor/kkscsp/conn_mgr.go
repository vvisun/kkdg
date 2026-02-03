package kkscsp

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

type ConnMgr struct {
	conns sync.Map // map[kknet.CONN_ID]kknet.IConn //连接ID -> 连接
}

func newConnMgr() *ConnMgr {
	return &ConnMgr{
		conns: sync.Map{},
	}
}

func (cm *ConnMgr) AddConn(conn kknet.IConn) {
	if conn == nil {
		return
	}
	cm.conns.Store(conn.ID(), conn)
}

func (cm *ConnMgr) RemoveConn(connID kknet.CONN_ID) {
	cm.conns.Delete(connID)
}

func (cm *ConnMgr) GetConn(connID kknet.CONN_ID) (kknet.IConn, bool) {
	v, ok := cm.conns.Load(connID)
	if !ok {
		return nil, false
	}
	return v.(kknet.IConn), true
}
