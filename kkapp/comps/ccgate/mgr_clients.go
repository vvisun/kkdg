package ccgate

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

// 客户端信息。
//
//	连接ID，会话ID，逻辑节点【节点ID、节点类型、用户ID(是否已登录该节点)】绑定表
type clientInfo struct {
	// nodeType -> *clientLogicItem。
	//  本客户端链接的逻辑节点字典。
	//  同一个客户端可能链接不同类型的逻辑服，比如充值服，大厅服，游戏服，聊天服等。
	clientBindTbl *clientBindTable
	// 客户端会话ID
	sessionId string
	// 客户端连接ID
	connId kknet.CONN_ID
}

func newClientInfo(connId kknet.CONN_ID, sessionId string) *clientInfo {
	return &clientInfo{
		connId:        connId,
		sessionId:     sessionId,
		clientBindTbl: newClientBindTable(),
	}
}

func (c *clientInfo) rangeLogicNodes(fn func(nodeType string, lgcInfo *clientLogicItem) bool) {
	c.clientBindTbl.rangeLogicItems(fn)
}

// 获取本客户端链接的nodeType类型的逻辑节点信息。
func (c *clientInfo) getLogicNode(nodeType string) *clientLogicItem {
	return c.clientBindTbl.getLogicItem(nodeType)
}

// 为本客户端分配nodeType类型的逻辑节点。
func (c *clientInfo) bindLogicNode(nodeType string, nodeId string) *clientLogicItem {
	lgcInfo := c.getLogicNode(nodeType)
	if lgcInfo != nil {
		return lgcInfo
	}
	lgcInfo = c.clientBindTbl.bindLogicItem(nodeType, nodeId)
	return lgcInfo
}

//------------------------------------------------------------

// 客户端管理器，管理连接级的逻辑服分配情况。
// 生命周期为连接级，新建连接时添加，连接断开时移除。
type clientManager struct {
	muMaps     sync.RWMutex
	connMap    map[kknet.CONN_ID]*clientInfo //kknet.CONN_ID -> *clientInfo
	sessionMap map[string]*clientInfo        //sessionId -> *clientInfo
}

func newClientManager() *clientManager {
	return &clientManager{
		connMap:    make(map[kknet.CONN_ID]*clientInfo),
		sessionMap: make(map[string]*clientInfo),
	}
}

// 添加连接connId的客户端。
func (m *clientManager) addClient(connId kknet.CONN_ID, sessionId string) *clientInfo {
	if info := m.getClientByConnId(connId); info != nil {
		info.sessionId = sessionId
		return info
	}
	cliInfo := newClientInfo(connId, sessionId)
	m.muMaps.Lock()
	m.connMap[connId] = cliInfo
	m.sessionMap[sessionId] = cliInfo
	m.muMaps.Unlock()
	return cliInfo
}

// 移除连接connId的客户端。
func (m *clientManager) removeClient(connId kknet.CONN_ID) {
	cliInfo := m.getClientByConnId(connId)
	if cliInfo == nil {
		return
	}
	m.muMaps.Lock()
	sessionId := cliInfo.sessionId // sessionId = getSessionId(connId, gateNodeId)
	delete(m.connMap, connId)
	delete(m.sessionMap, sessionId)
	cliInfo.connId = kknet.NULL_CONN_ID
	cliInfo.sessionId = ""
	m.muMaps.Unlock()
}

// 根据connId获取客户端信息。
func (m *clientManager) getClientByConnId(connId kknet.CONN_ID) *clientInfo {
	m.muMaps.RLock()
	cliInfo, ok := m.connMap[connId]
	m.muMaps.RUnlock()
	if ok && cliInfo != nil {
		return cliInfo
	}
	return nil
}

// 根据sessionId获取客户端信息。
func (m *clientManager) getClientBySessionId(sessionId string) *clientInfo {
	m.muMaps.RLock()
	cliInfo, ok := m.sessionMap[sessionId]
	m.muMaps.RUnlock()
	if ok && cliInfo != nil {
		return cliInfo
	}
	return nil
}

// 为连接connId的客户端分配一个nodeType类型的逻辑节点。如果已分配，则返回已分配的逻辑节点信息。
func (m *clientManager) allocLogicNode(connId kknet.CONN_ID, nodeType string, nodeId string) *clientLogicItem {
	cliInfo := m.getClientByConnId(connId)
	if cliInfo == nil {
		return nil
	}
	return cliInfo.bindLogicNode(nodeType, nodeId)
}
