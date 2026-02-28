package ccgate

import "github.com/vvisun/kkdg/kknet"

// 客户端信息。
type clientInfo struct {
	connId    kknet.CONN_ID // 客户端连接ID
	userId    kknet.USER_ID // 用户ID
	sessionId string        // 客户端会话ID
	// key:nodeType。本客户端链接的逻辑节点列表。
	// 同一个客户端可能链接不同类型的逻辑服，比如充值服，大厅服，游戏服，聊天服等。
	logicNodeMap map[string]*logicNodeInfo
}

// 获取本客户端链接的nodeType类型的逻辑节点信息。
func (c *clientInfo) GetLogicNodeInfo(nodeType string) *logicNodeInfo {
	info, ok := c.logicNodeMap[nodeType]
	if !ok || info == nil {
		return nil
	}
	return info
}

// 为本客户端分配nodeType类型的逻辑节点。
func (c *clientInfo) AddLogicNodeInfo(nodeType string, info *logicNodeInfo) {
	if info == nil {
		return
	}
	c.logicNodeMap[nodeType] = info
}

func newClientInfo(connId kknet.CONN_ID, sessionId string) *clientInfo {
	return &clientInfo{
		connId:       connId,
		sessionId:    sessionId,
		logicNodeMap: make(map[string]*logicNodeInfo),
	}
}

//------------------------------------------------------------

// 客户端管理器。
type clientManager struct {
	clientMap map[kknet.CONN_ID]*clientInfo
	userMap   map[kknet.USER_ID]*clientInfo
}

func newClientManager() *clientManager {
	return &clientManager{
		clientMap: make(map[kknet.CONN_ID]*clientInfo),
		userMap:   make(map[kknet.USER_ID]*clientInfo),
	}
}

// 添加连接connId的客户端。
func (m *clientManager) AddClient(connId kknet.CONN_ID, sessionId string) *clientInfo {
	info := newClientInfo(connId, sessionId)
	m.clientMap[connId] = info
	return info
}

// 移除连接connId的客户端。
func (m *clientManager) RemoveClient(connId kknet.CONN_ID) {
	info, ok := m.clientMap[connId]
	if !ok || info == nil {
		return
	}
	delete(m.clientMap, connId)
	delete(m.userMap, info.userId)
}

// 根据connId获取客户端信息。
func (m *clientManager) GetClientInfo(connId kknet.CONN_ID) *clientInfo {
	info, ok := m.clientMap[connId]
	if !ok || info == nil {
		return nil
	}
	return info
}

// 根据userId获取客户端信息。
func (m *clientManager) GetClientInfoByUserId(userId kknet.USER_ID) *clientInfo {
	info, ok := m.userMap[userId]
	if !ok || info == nil {
		return nil
	}
	return info
}

// 为连接connId的客户端分配nodeType类型的逻辑节点。如果已分配，则返回已分配的逻辑节点信息。
func (m *clientManager) AddLogicNodeInfo(connId kknet.CONN_ID, nodeType string, nodeId string) *logicNodeInfo {
	info := m.clientMap[connId].GetLogicNodeInfo(nodeType)
	if info != nil {
		return info
	}
	info = newLogicNodeInfo(nodeId, nodeType)
	m.clientMap[connId].AddLogicNodeInfo(nodeType, info)
	return info
}

// 连接connId的客户端登录到本网关。
func (m *clientManager) Login(connId kknet.CONN_ID, userId kknet.USER_ID) bool {
	if userId == kknet.NULL_USER_ID {
		return false
	}
	info, ok := m.clientMap[connId]
	if !ok || info == nil {
		return false
	}
	info.userId = userId
	m.userMap[userId] = info
	return true
}

// 连接connId的客户端登录到nodeType类型的逻辑节点。
func (m *clientManager) LoginToLogicNode(connId kknet.CONN_ID, nodeType string, userId kknet.USER_ID) bool {
	if userId == kknet.NULL_USER_ID {
		return false
	}
	info := m.clientMap[connId].GetLogicNodeInfo(nodeType)
	if info == nil {
		return false
	}
	info.Login(userId)
	return true
}

//------------------------------------------------------------

// 逻辑节点信息。
type logicNodeInfo struct {
	userId   kknet.USER_ID // 用户ID。未登录时为0，登录后为实际用户ID。
	nodeId   string        // 逻辑节点ID
	nodeType string        // 逻辑节点类型
}

// 是否已登录到本逻辑节点
func (l *logicNodeInfo) IsLogin() bool {
	return l.userId != kknet.NULL_USER_ID
}

// 登录到本逻辑节点
func (l *logicNodeInfo) Login(userId kknet.USER_ID) {
	l.userId = userId
}

func newLogicNodeInfo(nodeId string, nodeType string) *logicNodeInfo {
	return &logicNodeInfo{
		nodeId:   nodeId,
		nodeType: nodeType,
	}
}
