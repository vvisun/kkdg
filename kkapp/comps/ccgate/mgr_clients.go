package ccgate

import (
	"strconv"
	"sync"

	"github.com/vvisun/kkdg/kkapp/user"
	"github.com/vvisun/kkdg/kknet"
)

//go:inline
func getSessionId(connID kknet.CONN_ID, gateNodeId string) string {
	return gateNodeId + "-" + strconv.FormatUint(connID, 10)
}

//------------------------------------------------------------

// 客户端的逻辑节点绑定信息。
//
//	节点ID，节点类型，用户ID(是否已登录该节点)
type clientLogicItem struct {
	userId   user.USER_ID // 用户ID。用于标识是否已登录到本逻辑节点
	nodeId   string       // 逻辑节点ID
	nodeType string       // 逻辑节点类型
}

// 是否已登录到本逻辑节点
func (l *clientLogicItem) isLogin() bool {
	return l.userId != user.NULL_USER_ID
}

func newClientLogicItem(nodeId string, nodeType string) *clientLogicItem {
	return &clientLogicItem{
		userId:   user.NULL_USER_ID,
		nodeId:   nodeId,
		nodeType: nodeType,
	}
}

//------------------------------------------------------------

// 客户端信息。
//
//	连接ID，用户ID，会话ID，逻辑节点【节点ID、节点类型、用户ID(是否已登录该节点)】绑定表
type clientInfo struct {
	// 客户端连接ID
	connId kknet.CONN_ID
	// 用户ID
	userId user.USER_ID
	// nodeType -> *clientLogicItem。
	//  本客户端链接的逻辑节点字典。
	//  同一个客户端可能链接不同类型的逻辑服，比如充值服，大厅服，游戏服，聊天服等。
	logicNodeMap sync.Map
	// 客户端会话ID
	sessionId string
}

func (c *clientInfo) rangeLogicNodes(fn func(nodeType string, lgcInfo *clientLogicItem) bool) {
	c.logicNodeMap.Range(func(key any, value any) bool {
		item := value.(*clientLogicItem)
		if item == nil {
			return true
		}
		return fn(key.(string), item)
	})
}

// 获取本客户端链接的nodeType类型的逻辑节点信息。
func (c *clientInfo) getLogicNode(nodeType string) *clientLogicItem {
	info, ok := c.logicNodeMap.Load(nodeType)
	if !ok || info == nil {
		return nil
	}
	return info.(*clientLogicItem)
}

// 为本客户端分配nodeType类型的逻辑节点。
func (c *clientInfo) bindLogicNode(nodeType string, nodeId string) *clientLogicItem {
	lgcInfo := c.getLogicNode(nodeType)
	if lgcInfo != nil {
		return lgcInfo
	}
	lgcInfo = newClientLogicItem(nodeId, nodeType)
	c.logicNodeMap.LoadOrStore(nodeType, lgcInfo)
	gLogicTotalMgr.onBindLogicNode(c.sessionId, nodeId, nodeType)
	return lgcInfo
}

// 解绑逻辑节点。
//
// 注意：
//   - 不能仅因为网关侧连接断开，就立刻从逻辑节点统计中摘除该客户端；
//   - 因为逻辑服处理玩家下线、踢人、重连迁移等操作可能慢于网关断连事件；
//   - 此时玩家可能仍停留在原逻辑服的玩家管理器中，如果网关过早摘除并重新分配到其他逻辑服，
//     会出现同一玩家短时间同时登录进两个不同逻辑服，导致玩家数据混乱。
//
// 建议的解绑时机：
//   - 上层玩家管理已经确认该玩家已从目标逻辑服安全移除；
//   - 然后再通过 rpc 或其它方式通知网关执行摘除。
func (c *clientInfo) unbindLogicNode(nodeType string) {
	// 这里可以向逻辑节点发动一次rpc请求，查看本客户端是否已从该逻辑节点下线。
	// 另一种方式是，逻辑节点主动推送上线/下线事件，网关侧监听并更新本地缓存。
	if lgcInfo := c.getLogicNode(nodeType); lgcInfo != nil {
		gLogicTotalMgr.onUnbindLogicNode(c.sessionId, lgcInfo.nodeId)
	}
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
//
//	管理网关侧连接/会话索引，以及用户ID与客户端的映射关系。
//	辅助消息路由转发到对应的逻辑节点、顶号踢人、记录连接/玩家的逻辑服绑定信息。
type clientManager struct {
	muMaps     sync.RWMutex
	connMap    map[kknet.CONN_ID]*clientInfo //kknet.CONN_ID -> *clientInfo
	sessionMap map[string]*clientInfo        //sessionId -> *clientInfo
	userMap    map[user.USER_ID]*clientInfo  //user.USER_ID -> *clientInfo
}

func newClientManager() *clientManager {
	return &clientManager{
		connMap:    make(map[kknet.CONN_ID]*clientInfo),
		sessionMap: make(map[string]*clientInfo),
		userMap:    make(map[user.USER_ID]*clientInfo),
	}
}

// 添加连接connId的客户端。
func (m *clientManager) addClient(connId kknet.CONN_ID, sessionId string) *clientInfo {
	if info := m.getClient(connId); info != nil {
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
//
// 注意：
//   - 这里只移除网关侧连接/会话索引，不会自动解绑 logicNodeMap，也不会自动回收 gLogicTotalMgr 中的绑定统计；
//   - 是否可以安全解绑，依赖于上层玩家管理确认该玩家是否仍停留在原逻辑服；
//   - 如果需要解绑，应在逻辑服确认玩家已安全移除后，再通知网关调用 unbindLogicNode。
func (m *clientManager) removeClient(connId kknet.CONN_ID) {
	cliInfo := m.getClient(connId)
	if cliInfo == nil {
		return
	}
	uid := cliInfo.userId
	sessionId := cliInfo.sessionId
	m.muMaps.Lock()
	delete(m.connMap, connId)
	delete(m.sessionMap, sessionId)
	delete(m.userMap, uid)
	m.muMaps.Unlock()
}

// 根据connId获取客户端信息。
func (m *clientManager) getClient(connId kknet.CONN_ID) *clientInfo {
	m.muMaps.RLock()
	cliInfo, ok := m.connMap[connId]
	m.muMaps.RUnlock()
	if ok && cliInfo != nil {
		return cliInfo
	}
	return nil
}

// 根据userId获取客户端信息。
func (m *clientManager) getClientByUserId(userId user.USER_ID) *clientInfo {
	m.muMaps.RLock()
	cliInfo, ok := m.userMap[userId]
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
	cliInfo := m.getClient(connId)
	if cliInfo == nil {
		return nil
	}
	return cliInfo.bindLogicNode(nodeType, nodeId)
}

// 检查是否需要踢出旧用户。如果需要踢出，则返回需要踢出的connId。
func (m *clientManager) checkKickFromGate(connId kknet.CONN_ID, userId user.USER_ID) kknet.CONN_ID {
	if userId == user.NULL_USER_ID {
		return kknet.NULL_CONN_ID
	}
	cliInfo := m.getClient(connId)
	if cliInfo == nil {
		return kknet.NULL_CONN_ID
	}
	if cliInfo.userId != user.NULL_USER_ID && (cliInfo.connId != connId || cliInfo.userId != userId) {
		return cliInfo.connId
	}
	usrCliInfo := m.getClientByUserId(userId)
	if usrCliInfo != nil && (usrCliInfo.connId != connId || usrCliInfo.userId != userId) {
		return usrCliInfo.connId
	}
	return kknet.NULL_CONN_ID
}

// 连接connId的客户端登录到本网关。如果需要踢出旧用户，则返回需要踢出的用户connId。
func (m *clientManager) loginToGate(connId kknet.CONN_ID, userId user.USER_ID) (bool, kknet.CONN_ID) {
	if userId == user.NULL_USER_ID {
		return false, kknet.NULL_CONN_ID
	}
	cliInfo := m.getClient(connId)
	if cliInfo == nil {
		return false, kknet.NULL_CONN_ID
	}

	kickConnId := m.checkKickFromGate(connId, userId)
	if kickConnId != kknet.NULL_CONN_ID {
		m.removeClient(kickConnId)
	}

	m.muMaps.Lock()
	cliInfo.userId = userId
	m.userMap[userId] = cliInfo
	m.muMaps.Unlock()
	return true, kickConnId
}

func (m *clientManager) checkKickFromLogicNode(sessionId string, nodeType string, userId user.USER_ID) *clientLogicItem {
	if userId == user.NULL_USER_ID {
		return nil
	}

	if cliInfo := m.getClientBySessionId(sessionId); cliInfo != nil {
		lgcInfo := cliInfo.getLogicNode(nodeType)
		if lgcInfo == nil {
			return nil
		}
		if lgcInfo.userId != user.NULL_USER_ID && lgcInfo.userId != userId {
			return lgcInfo
		}
	}

	if cliInfo := m.getClientByUserId(userId); cliInfo != nil {
		lgcInfo := cliInfo.getLogicNode(nodeType)
		if lgcInfo == nil {
			return nil
		}
		if lgcInfo.userId != user.NULL_USER_ID && lgcInfo.userId != userId {
			return lgcInfo
		}
	}

	return nil
}

// 连接connId的客户端登录到nodeType类型的逻辑节点。
func (m *clientManager) loginToLogicNode(sessionId string, nodeType string, userId user.USER_ID) bool {
	if userId == user.NULL_USER_ID {
		return false
	}
	cliInfo := m.getClientBySessionId(sessionId)
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

// 连接connId的客户端登出nodeType类型的逻辑节点。
func (m *clientManager) logoutFromLogicNode(sessionId string, nodeType string) bool {
	cliInfo := m.getClientBySessionId(sessionId)
	if cliInfo == nil {
		return false
	}
	lgcInfo := cliInfo.getLogicNode(nodeType)
	if lgcInfo == nil {
		return false
	}
	lgcInfo.userId = user.NULL_USER_ID
	return true
}
