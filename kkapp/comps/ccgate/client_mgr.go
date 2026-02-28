package ccgate

import (
	"strconv"
	"sync"

	"github.com/vvisun/kkdg/kknet"
)

//go:inline
func getSessionId(connID kknet.CONN_ID) string {
	return strconv.FormatUint(connID, 10)
}

// 客户端信息。
type clientInfo struct {
	connId    kknet.CONN_ID // 客户端连接ID
	userId    kknet.USER_ID // 用户ID
	sessionId string        // 客户端会话ID
	// key:nodeType。本客户端链接的逻辑节点列表。
	// 同一个客户端可能链接不同类型的逻辑服，比如充值服，大厅服，游戏服，聊天服等。
	logicNodeMap sync.Map // map[string]*logicNodeInfo
}

// 获取本客户端链接的nodeType类型的逻辑节点信息。
func (c *clientInfo) GetLogicNodeInfo(nodeType string) *logicNodeInfo {
	info, ok := c.logicNodeMap.Load(nodeType)
	if !ok || info == nil {
		return nil
	}
	return info.(*logicNodeInfo)
}

// 为本客户端分配nodeType类型的逻辑节点。
func (c *clientInfo) AddLogicNodeInfo(nodeType string, info *logicNodeInfo) {
	if info == nil {
		return
	}
	c.logicNodeMap.Store(nodeType, info)
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

func newClientManager() *clientManager {
	return &clientManager{}
}

// 添加连接connId的客户端。
func (m *clientManager) AddClient(connId kknet.CONN_ID, sessionId string) *clientInfo {
	cliInfo := newClientInfo(connId, sessionId)
	m.clientMap.Store(connId, cliInfo)
	return cliInfo
}

// 移除连接connId的客户端。
func (m *clientManager) RemoveClient(connId kknet.CONN_ID) {
	cliInfo := m.GetClientInfo(connId)
	if cliInfo == nil {
		return
	}
	m.clientMap.Delete(connId)
	m.userMap.Delete(cliInfo.userId)
}

// 根据connId获取客户端信息。
func (m *clientManager) GetClientInfo(connId kknet.CONN_ID) *clientInfo {
	cliInfo, ok := m.clientMap.Load(connId)
	if !ok || cliInfo == nil || cliInfo.(*clientInfo) == nil {
		return nil
	}
	return cliInfo.(*clientInfo)
}

// 根据userId获取客户端信息。
func (m *clientManager) GetClientInfoByUserId(userId kknet.USER_ID) *clientInfo {
	cliInfo, ok := m.userMap.Load(userId)
	if !ok || cliInfo == nil || cliInfo.(*clientInfo) == nil {
		return nil
	}
	return cliInfo.(*clientInfo)
}

// 为连接connId的客户端分配nodeType类型的逻辑节点。如果已分配，则返回已分配的逻辑节点信息。
func (m *clientManager) AddLogicNodeInfo(connId kknet.CONN_ID, nodeType string, nodeId string) *logicNodeInfo {
	cliInfo := m.GetClientInfo(connId)
	if cliInfo == nil {
		return nil
	}
	info := cliInfo.GetLogicNodeInfo(nodeType)
	if info != nil {
		return info
	}
	info = newLogicNodeInfo(nodeId, nodeType)
	cliInfo.AddLogicNodeInfo(nodeType, info)
	return info
}

// 连接connId的客户端登录到本网关。
func (m *clientManager) Login(connId kknet.CONN_ID, userId kknet.USER_ID) bool {
	if userId == kknet.NULL_USER_ID {
		return false
	}
	cliInfo := m.GetClientInfo(connId)
	if cliInfo == nil {
		return false
	}
	cliInfo.userId = userId
	m.userMap.Store(userId, cliInfo)
	return true
}

// 连接connId的客户端登录到nodeType类型的逻辑节点。
func (m *clientManager) LoginToLogicNode(connId kknet.CONN_ID, nodeType string, userId kknet.USER_ID) bool {
	if userId == kknet.NULL_USER_ID {
		return false
	}
	cliInfo := m.GetClientInfo(connId)
	if cliInfo == nil {
		return false
	}
	info := cliInfo.GetLogicNodeInfo(nodeType)
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
		userId:   kknet.NULL_USER_ID,
		nodeId:   nodeId,
		nodeType: nodeType,
	}
}
