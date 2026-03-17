package ccgate

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/user"
	"github.com/vvisun/kkdg/utils/xcall"
)

// 客户端的逻辑节点绑定信息。
//
//	节点ID，节点类型，用户ID(是否已登录该节点)
type clientLogicItem struct {
	nodeId   string // 逻辑节点ID
	nodeType string // 逻辑节点类型
}

func newClientLogicItem(nodeId string, nodeType string) *clientLogicItem {
	return &clientLogicItem{
		nodeId:   nodeId,
		nodeType: nodeType,
	}
}

//------------------------------------------------------------

// 客户端的逻辑节点绑定表。nodeType -> *clientLogicItem
type clientBindTable struct {
	mu           sync.Mutex
	logicItemMap map[string]*clientLogicItem // nodeType -> *clientLogicItem
}

func newClientBindTable() *clientBindTable {
	return &clientBindTable{
		logicItemMap: make(map[string]*clientLogicItem),
	}
}

// 绑定逻辑节点。如果已绑定，则返回已绑定的逻辑节点信息。
func (t *clientBindTable) bindLogicItem(nodeType string, nodeId string) *clientLogicItem {
	t.mu.Lock()
	if logicItem, ok := t.logicItemMap[nodeType]; ok {
		t.mu.Unlock()
		return logicItem
	}
	logicItem := newClientLogicItem(nodeId, nodeType)
	t.logicItemMap[nodeType] = logicItem
	t.mu.Unlock()
	return logicItem
}

// 解绑逻辑节点。
func (t *clientBindTable) unbindLogicItem(nodeType string) {
	t.mu.Lock()
	delete(t.logicItemMap, nodeType)
	t.mu.Unlock()
}

// 获取逻辑节点信息。
func (t *clientBindTable) getLogicItem(nodeType string) *clientLogicItem {
	t.mu.Lock()
	logicItem, ok := t.logicItemMap[nodeType]
	t.mu.Unlock()
	if !ok {
		return nil
	}
	return logicItem
}

// 遍历逻辑节点绑定表。fn返回false时停止遍历。
func (t *clientBindTable) rangeLogicItems(fn func(nodeType string, logicItem *clientLogicItem) bool) {
	if fn == nil || len(t.logicItemMap) == 0 {
		return
	}

	items := make([]*clientLogicItem, 0, len(t.logicItemMap))
	t.mu.Lock()
	for nodeType, logicItem := range t.logicItemMap {
		if logicItem == nil {
			delete(t.logicItemMap, nodeType)
			continue
		}
		items = append(items, logicItem)
	}
	t.mu.Unlock()

	xcall.SafeCall(
		func() {
			for _, logicItem := range items {
				if !fn(logicItem.nodeType, logicItem) {
					break
				}
			}
		},
	)
}

//------------------------------------------------------------

// 这里为了记住用户已分配的逻辑服，方便后续用户重新登录时，能接入之前的逻辑服。
// 如果不记录，用户重新登录时可能分配到新的逻辑服，这时候旧的逻辑服可能还在处理用户逻辑，
// 导致用户登入多个同类逻辑服造成状态和数据混乱，除非业务逻辑本身不依赖顺序性。
type logicBindManager struct {
	mu sync.Mutex
	// sessionId -> *clientBindTable
	//  生命周期为会话级。
	//  会话断开时，移除绑定关系。为会话分配逻辑服成功时，添加绑定关系。
	sessionTable map[string]*clientBindTable
	// userId -> *clientBindTable
	//  生命周期为用户级。
	//  用户登出某逻辑服时，移除与该逻辑服的绑定关系。
	//  用户登录某逻辑服成功时，添加与该逻辑服的绑定关系。
	userTable map[user.USER_ID]*clientBindTable
}

func newLogicBindManager() *logicBindManager {
	return &logicBindManager{
		sessionTable: make(map[string]*clientBindTable),
		userTable:    make(map[user.USER_ID]*clientBindTable),
	}
}

// 为会话分配某类逻辑服成功时，添加与该逻辑服的绑定关系。
func (m *logicBindManager) sessionBind(sessionId string, nodeType string, nodeId string) *clientLogicItem {
	m.mu.Lock()
	bindTable, ok := m.sessionTable[sessionId]
	if !ok {
		bindTable = newClientBindTable()
		m.sessionTable[sessionId] = bindTable
		m.mu.Unlock()
		return bindTable.bindLogicItem(nodeType, nodeId)
	}
	m.mu.Unlock()
	return bindTable.bindLogicItem(nodeType, nodeId)
}

// 用户登录某逻辑服时，添加与该逻辑服的绑定关系。
func (m *logicBindManager) userBind(userId user.USER_ID, nodeType string, nodeId string) *clientLogicItem {
	m.mu.Lock()
	bindTable, ok := m.userTable[userId]
	if !ok {
		bindTable = newClientBindTable()
		m.userTable[userId] = bindTable
		m.mu.Unlock()
		return bindTable.bindLogicItem(nodeType, nodeId)
	}
	m.mu.Unlock()
	return bindTable.bindLogicItem(nodeType, nodeId)
}

// 解除会话与某类逻辑服的绑定关系。
func (m *logicBindManager) sessionUnbind(sessionId string, nodeType string) {
	m.mu.Lock()
	bindTbl, ok := m.sessionTable[sessionId]
	if !ok {
		m.mu.Unlock()
		return
	}
	bindTbl.unbindLogicItem(nodeType)
	if len(bindTbl.logicItemMap) == 0 {
		delete(m.sessionTable, sessionId)
	}
	m.mu.Unlock()
}

// 用户登出某逻辑服时，移除与该逻辑服的绑定关系。
func (m *logicBindManager) userUnbind(userId user.USER_ID, nodeType string) {
	m.mu.Lock()
	bindTbl, ok := m.userTable[userId]
	if !ok {
		m.mu.Unlock()
		return
	}
	bindTbl.unbindLogicItem(nodeType)
	if len(bindTbl.logicItemMap) == 0 {
		delete(m.userTable, userId)
	}
	m.mu.Unlock()
}

// 获取会话与某类逻辑服的绑定信息。
func (m *logicBindManager) getLogicItemBySessionId(sessionId string, nodeType string) *clientLogicItem {
	m.mu.Lock()
	bindTbl, ok := m.sessionTable[sessionId]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return bindTbl.getLogicItem(nodeType)
}

// 获取用户与某类逻辑服的绑定信息。
func (m *logicBindManager) getLogicItemByUserId(userId user.USER_ID, nodeType string) *clientLogicItem {
	m.mu.Lock()
	bindTbl, ok := m.userTable[userId]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return bindTbl.getLogicItem(nodeType)
}

// 获取会话的逻辑服绑定表。
func (m *logicBindManager) getSessionBindTable(sessionId string) *clientBindTable {
	m.mu.Lock()
	bindTbl, ok := m.sessionTable[sessionId]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return bindTbl
}

// 获取用户的逻辑服绑定表。
func (m *logicBindManager) getUserBindTable(userId user.USER_ID) *clientBindTable {
	m.mu.Lock()
	bindTbl, ok := m.userTable[userId]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return bindTbl
}

// 会话断开时，移除该会话的绑定表。
func (m *logicBindManager) onSessionDisconnect(sessionId string) {
	m.mu.Lock()
	delete(m.sessionTable, sessionId)
	m.mu.Unlock()
}
