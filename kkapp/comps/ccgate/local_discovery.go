package ccgate

import "sync"

type session2nodeType map[string]string

// 实时统计每个逻辑节点上的会话数量，用于负载均衡。
// 相当于本地简易版discovery，用于负载均衡。
type localDidcovery struct {
	logicNodeTable map[string]session2nodeType // map[nodeId]session2nodeType
	mu             sync.RWMutex
}

func newLocalDiscovery() *localDidcovery {
	return &localDidcovery{
		logicNodeTable: make(map[string]session2nodeType),
	}
}

// 为 sessionId 绑定逻辑节点时更新统计。
func (m *localDidcovery) onBindLogicNode(sessionId string, nodeType string, nodeId string) {
	m.mu.Lock()
	nodeMap, ok := m.logicNodeTable[nodeId]
	if !ok {
		nodeMap = make(session2nodeType)
		m.logicNodeTable[nodeId] = nodeMap
	}
	nodeMap[sessionId] = nodeType
	m.mu.Unlock()
}

// 为 sessionId 解绑逻辑节点时更新统计。
// 调用方应确保上层玩家管理已经确认该玩家可安全从该逻辑服摘除。
func (m *localDidcovery) onUnbindLogicNode(sessionId string, nodeId string) {
	m.mu.Lock()
	if nodeMap, ok := m.logicNodeTable[nodeId]; ok {
		delete(nodeMap, sessionId)
		if len(nodeMap) == 0 {
			delete(m.logicNodeTable, nodeId)
		}
	}
	m.mu.Unlock()
}

// 获取nodeId上的session数量
func (m *localDidcovery) getSessionCount(nodeId string) int {
	m.mu.RLock()
	nodeTypeMap, ok := m.logicNodeTable[nodeId]
	if !ok {
		m.mu.RUnlock()
		return 0
	}
	count := len(nodeTypeMap)
	m.mu.RUnlock()
	return count
}
