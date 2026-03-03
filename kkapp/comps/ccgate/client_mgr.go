package ccgate

import (
	"strconv"
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

//go:inline
func getSessionId(connID kknet.CONN_ID, gateNodeId string) string {
	return gateNodeId + "-" + strconv.FormatUint(connID, 10)
}

// 客户端信息。
type clientInfo struct {
	connId    kknet.CONN_ID // 客户端连接ID
	userId    kknet.USER_ID // 用户ID
	sessionId string        // 客户端会话ID
	// key: nodeType。
	// 本客户端链接的逻辑节点列表。
	// 同一个客户端可能链接不同类型的逻辑服，比如充值服，大厅服，游戏服，聊天服等。
	logicNodeMap sync.Map // map[string]*logicNodeInfo
}

// 获取本客户端链接的nodeType类型的逻辑节点信息。
func (c *clientInfo) getLogicNode(nodeType string) *logicNodeInfo {
	info, ok := c.logicNodeMap.Load(nodeType)
	if !ok || info == nil {
		return nil
	}
	return info.(*logicNodeInfo)
}

// 为本客户端分配nodeType类型的逻辑节点。
func (c *clientInfo) allocLogicNode(nodeType string, nodeId string) *logicNodeInfo {
	lgcInfo := c.getLogicNode(nodeType)
	if lgcInfo != nil {
		return lgcInfo
	}
	lgcInfo = newLogicNodeInfo(nodeId, nodeType)
	c.logicNodeMap.Store(nodeType, lgcInfo)
	return lgcInfo
}

// 解绑逻辑节点
func (c *clientInfo) removeLogicNode(nodeType string) {
	c.logicNodeMap.Delete(nodeType)
}

func newClientInfo(connId kknet.CONN_ID, sessionId string) *clientInfo {
	return &clientInfo{
		connId:    connId,
		sessionId: sessionId,
	}
}

//------------------------------------------------------------

// 客户端管理器。
type clientManager struct {
	clientMap sync.Map // map[kknet.CONN_ID]*clientInfo
	userMap   sync.Map // map[kknet.USER_ID]*clientInfo
}

// 添加连接connId的客户端。
func (m *clientManager) addClient(connId kknet.CONN_ID, sessionId string) *clientInfo {
	cliInfo := newClientInfo(connId, sessionId)
	m.clientMap.Store(connId, cliInfo)
	return cliInfo
}

// 移除连接connId的客户端。
func (m *clientManager) removeClient(connId kknet.CONN_ID) {
	cliInfo := m.getClient(connId)
	if cliInfo == nil {
		return
	}
	uid := cliInfo.userId
	m.clientMap.Delete(connId)
	m.userMap.Delete(uid)
}

// 根据connId获取客户端信息。
func (m *clientManager) getClient(connId kknet.CONN_ID) *clientInfo {
	cliInfo, ok := m.clientMap.Load(connId)
	if ok && cliInfo != nil {
		return cliInfo.(*clientInfo)
	}
	return nil
}

// 根据userId获取客户端信息。
func (m *clientManager) getClientByUserId(userId kknet.USER_ID) *clientInfo {
	cliInfo, ok := m.userMap.Load(userId)
	if ok && cliInfo != nil {
		return cliInfo.(*clientInfo)
	}
	return nil
}

// 为连接connId的客户端分配一个nodeType类型的逻辑节点。如果已分配，则返回已分配的逻辑节点信息。
func (m *clientManager) allocLogicNode(connId kknet.CONN_ID, nodeType string, nodeId string) *logicNodeInfo {
	cliInfo := m.getClient(connId)
	if cliInfo == nil {
		return nil
	}
	return cliInfo.allocLogicNode(nodeType, nodeId)
}

// 连接connId的客户端登录到nodeType类型的逻辑节点。
func (m *clientManager) loginToLogicNode(connId kknet.CONN_ID, nodeType string, userId kknet.USER_ID) bool {
	if userId == kknet.NULL_USER_ID {
		return false
	}
	cliInfo := m.getClient(connId)
	if cliInfo == nil {
		return false
	}
	lgcInfo := cliInfo.getLogicNode(nodeType)
	if lgcInfo == nil {
		return false
	}
	lgcInfo.userId = userId
	return true
}

// 检查是否需要踢出旧用户。如果需要踢出，则返回需要踢出的connId。
func (m *clientManager) checkKickOutUser(connId kknet.CONN_ID, userId kknet.USER_ID) kknet.CONN_ID {
	if userId == kknet.NULL_USER_ID {
		return kknet.NULL_CONN_ID
	}
	cliInfo := m.getClient(connId)
	if cliInfo == nil {
		return kknet.NULL_CONN_ID
	}
	if cliInfo.userId != kknet.NULL_USER_ID && (cliInfo.connId != connId || cliInfo.userId != userId) {
		return cliInfo.connId
	}
	usrCliInfo := m.getClientByUserId(userId)
	if usrCliInfo != nil && (usrCliInfo.connId != connId || usrCliInfo.userId != userId) {
		return usrCliInfo.connId
	}
	return kknet.NULL_CONN_ID
}

// 连接connId的客户端登录到本网关。如果需要踢出旧用户，则返回需要踢出的用户connId。
func (m *clientManager) loginToGate(connId kknet.CONN_ID, userId kknet.USER_ID) (bool, kknet.CONN_ID) {
	if userId == kknet.NULL_USER_ID {
		return false, kknet.NULL_CONN_ID
	}
	cliInfo := m.getClient(connId)
	if cliInfo == nil {
		return false, kknet.NULL_CONN_ID
	}

	kickConnId := m.checkKickOutUser(connId, userId)
	if kickConnId != kknet.NULL_CONN_ID {
		m.removeClient(kickConnId)
	}

	cliInfo.userId = userId
	m.userMap.Store(userId, cliInfo)
	return true, kickConnId
}

//------------------------------------------------------------

// 逻辑节点信息。
type logicNodeInfo struct {
	userId   kknet.USER_ID // 用户ID。未登录时为0，登录后为实际用户ID。
	nodeId   string        // 逻辑节点ID
	nodeType string        // 逻辑节点类型
}

// 是否已登录到本逻辑节点
func (l *logicNodeInfo) isLogin() bool {
	return l.userId != kknet.NULL_USER_ID
}

func newLogicNodeInfo(nodeId string, nodeType string) *logicNodeInfo {
	return &logicNodeInfo{
		userId:   kknet.NULL_USER_ID,
		nodeId:   nodeId,
		nodeType: nodeType,
	}
}
