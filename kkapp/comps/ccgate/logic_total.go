package ccgate

import "sync"

type session2nodeType map[string]string

var gLogicTotalMgr = newLogicTotalManager()

// 逻辑节点统计
type logicTotalManager struct {
	logicNodeTable   map[string]session2nodeType // map[nodeId]session2nodeType
	logicNodeTableMu sync.RWMutex
}

func newLogicTotalManager() *logicTotalManager {
	return &logicTotalManager{
		logicNodeTable: make(map[string]session2nodeType),
	}
}

// 为sessionId分配逻辑节点时
func (m *logicTotalManager) onBindLogicNode(sessionId string, nodeId string, nodeType string) {
	m.logicNodeTableMu.Lock()
	nodeMap, ok := m.logicNodeTable[nodeId]
	if !ok {
		nodeMap = make(session2nodeType)
		m.logicNodeTable[nodeId] = nodeMap
	}
	nodeMap[sessionId] = nodeType
	m.logicNodeTableMu.Unlock()
}

// 为sessionId解绑逻辑节点时
func (m *logicTotalManager) onUnbindLogicNode(sessionId string, nodeId string) {
	m.logicNodeTableMu.Lock()
	if nodeMap, ok := m.logicNodeTable[nodeId]; ok {
		delete(nodeMap, sessionId)
		if len(nodeMap) == 0 {
			delete(m.logicNodeTable, nodeId)
		}
	}
	m.logicNodeTableMu.Unlock()
}

// 获取nodeId上的session数量
func (m *logicTotalManager) GetSessionCount(nodeId string) int {
	m.logicNodeTableMu.RLock()
	nodeTypeMap, ok := m.logicNodeTable[nodeId]
	if !ok {
		m.logicNodeTableMu.RUnlock()
		return 0
	}
	count := len(nodeTypeMap)
	m.logicNodeTableMu.RUnlock()
	return count
}
