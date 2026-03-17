package ccgate

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

// 客户端连接管理器。
//  1. 生命周期为连接级。新建连接时添加，连接断开时移除。
//  2. 主要用于管理网关侧connId和sessionId的映射关系，以及索引所有连接（按connId和sessionId）
type clientManager struct {
	muMaps     sync.RWMutex
	connMap    map[kknet.CONN_ID]string //kknet.CONN_ID -> sessionId
	sessionMap map[string]kknet.CONN_ID //sessionId -> kknet.CONN_ID
}

func newClientManager() *clientManager {
	return &clientManager{
		connMap:    make(map[kknet.CONN_ID]string),
		sessionMap: make(map[string]kknet.CONN_ID),
	}
}

// 添加连接。
func (m *clientManager) addClient(connId kknet.CONN_ID, sessionId string) {
	m.muMaps.Lock()
	m.connMap[connId] = sessionId
	m.sessionMap[sessionId] = connId
	m.muMaps.Unlock()
}

// 移除连接。
func (m *clientManager) removeClient(connId kknet.CONN_ID) {
	m.muMaps.Lock()
	sessionId, ok := m.connMap[connId]
	if !ok {
		m.muMaps.Unlock()
		return
	}
	delete(m.connMap, connId)
	delete(m.sessionMap, sessionId)
	m.muMaps.Unlock()
}

// 根据connId获取sessionId
func (m *clientManager) getSessionByConnId(connId kknet.CONN_ID) string {
	m.muMaps.RLock()
	sessionId, ok := m.connMap[connId]
	m.muMaps.RUnlock()
	if ok {
		return sessionId
	}
	return ""
}

// 根据sessionId获取connId
func (m *clientManager) getConnIdBySessionId(sessionId string) kknet.CONN_ID {
	m.muMaps.RLock()
	connId, ok := m.sessionMap[sessionId]
	m.muMaps.RUnlock()
	if ok {
		return connId
	}
	return kknet.NULL_CONN_ID
}
