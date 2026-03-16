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
	mu       sync.Mutex
	userId   user.USER_ID // 用户ID。用于标识是否已登录到本逻辑节点
	nodeId   string       // 逻辑节点ID
	nodeType string       // 逻辑节点类型
}

func (l *clientLogicItem) login(userId user.USER_ID) {
	l.mu.Lock()
	l.userId = userId
	l.mu.Unlock()
}

func (l *clientLogicItem) logout() {
	l.mu.Lock()
	l.userId = user.NULL_USER_ID
	l.mu.Unlock()
}

// 是否已登录到本逻辑节点
func (l *clientLogicItem) isLogin() bool {
	l.mu.Lock()
	ok := l.userId != user.NULL_USER_ID
	l.mu.Unlock()
	return ok
}

func newClientLogicItem(nodeId string, nodeType string) *clientLogicItem {
	return &clientLogicItem{
		userId:   user.NULL_USER_ID,
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

type logicBindManager struct {
	mu                sync.Mutex
	session2logicItem map[string]*clientBindTable       // sessionId -> *clientBindTable
	userId2logicItem  map[user.USER_ID]*clientBindTable // userId -> *clientBindTable
}

func newLogicBindManager() *logicBindManager {
	return &logicBindManager{
		session2logicItem: make(map[string]*clientBindTable),
		userId2logicItem:  make(map[user.USER_ID]*clientBindTable),
	}
}

func (m *logicBindManager) sessionBind(sessionId string, nodeType string, nodeId string) *clientLogicItem {
	m.mu.Lock()
	bindTable, ok := m.session2logicItem[sessionId]
	if !ok {
		bindTable = newClientBindTable()
		m.session2logicItem[sessionId] = bindTable
		m.mu.Unlock()
		return bindTable.bindLogicItem(nodeType, nodeId)
	}
	m.mu.Unlock()
	return bindTable.bindLogicItem(nodeType, nodeId)
}

func (m *logicBindManager) sessionUnbind(sessionId string) {
	m.mu.Lock()
	delete(m.session2logicItem, sessionId)
	m.mu.Unlock()
}

func (m *logicBindManager) userBind(userId user.USER_ID, nodeType string, nodeId string) *clientLogicItem {
	m.mu.Lock()
	bindTable, ok := m.userId2logicItem[userId]
	if !ok {
		bindTable = newClientBindTable()
		m.userId2logicItem[userId] = bindTable
		m.mu.Unlock()
		return bindTable.bindLogicItem(nodeType, nodeId)
	}
	m.mu.Unlock()
	return bindTable.bindLogicItem(nodeType, nodeId)
}

func (m *logicBindManager) userUnbind(userId user.USER_ID) {
	m.mu.Lock()
	delete(m.userId2logicItem, userId)
	m.mu.Unlock()
}

func (m *logicBindManager) getLogicItemBySessionId(sessionId string, nodeType string) *clientLogicItem {
	m.mu.Lock()
	bindTbl, ok := m.session2logicItem[sessionId]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return bindTbl.getLogicItem(nodeType)
}

func (m *logicBindManager) getLogicItemByUserId(userId user.USER_ID, nodeType string) *clientLogicItem {
	m.mu.Lock()
	bindTbl, ok := m.userId2logicItem[userId]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	return bindTbl.getLogicItem(nodeType)
}
