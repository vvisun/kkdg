package ccgate

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

// 客户端管理器，管理连接级的逻辑服分配情况。
// 生命周期为连接级，新建连接时添加，连接断开时移除。
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

// 添加连接connId的客户端。
func (m *clientManager) addClient(connId kknet.CONN_ID, sessionId string) {
	m.muMaps.Lock()
	m.connMap[connId] = sessionId
	m.sessionMap[sessionId] = connId
	m.muMaps.Unlock()
}

// 移除连接connId的客户端。
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

func (m *clientManager) getSessionByConnId(connId kknet.CONN_ID) string {
	m.muMaps.RLock()
	sessionId, ok := m.connMap[connId]
	m.muMaps.RUnlock()
	if ok {
		return sessionId
	}
	return ""
}

func (m *clientManager) getConnIdBySessionId(sessionId string) kknet.CONN_ID {
	m.muMaps.RLock()
	connId, ok := m.sessionMap[sessionId]
	m.muMaps.RUnlock()
	if ok {
		return connId
	}
	return kknet.NULL_CONN_ID
}
