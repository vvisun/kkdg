package ccgate

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

// 客户端管理器。
//  1. 生命周期为连接级。新建连接时添加，连接断开时或被踢出会话时移除。
//  2. 管理所有有效的客户端。
//  3. 某客户端被踢出会话时，从管理器中移除，便能做到不再接收被踢客户端的消息。
//  4. 被踢出的客户端，后续可以根据connId或sessionId查找kknet.IConnManager或gate.sessionMgr，进行消息反馈和关闭连接。
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

// 添加客户端。客户端连接建立时调用。
// add client.
func (m *clientManager) addClient(connId kknet.CONN_ID, sessionId string) {
	m.muMaps.Lock()
	m.connMap[connId] = sessionId
	m.sessionMap[sessionId] = connId
	m.muMaps.Unlock()
}

// 移除客户端。客户端连接断开时或被踢出会话时调用。
// remove client.
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

// 根据connId获取sessionId。
// get sessionId by connId.
func (m *clientManager) getSessionByConnId(connId kknet.CONN_ID) string {
	m.muMaps.RLock()
	sessionId, ok := m.connMap[connId]
	m.muMaps.RUnlock()
	if ok {
		return sessionId
	}
	return ""
}
