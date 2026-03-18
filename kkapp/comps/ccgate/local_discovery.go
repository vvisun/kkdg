package ccgate

import "sync"

// sessionId + nodeType -> bool
type session2nodeType map[string]bool

// 实时统计每个逻辑节点上的会话数量，用于负载均衡。
// 相当于本地简易版discovery。
type localDiscovery struct {
	mu        sync.RWMutex
	nodeTable map[string]session2nodeType // map[nodeId]session2nodeType
}

func newLocalDiscovery() *localDiscovery {
	return &localDiscovery{
		nodeTable: make(map[string]session2nodeType, 256),
	}
}

// 为 sessionId 绑定逻辑节点时更新统计。update member weight
func (m *localDiscovery) onBindLogicNode(sessionId string, nodeType string, nodeId string) {
	m.mu.Lock()
	nodeMap, ok := m.nodeTable[nodeId]
	if !ok {
		nodeMap = make(session2nodeType)
		m.nodeTable[nodeId] = nodeMap
	}
	nodeMap[sessionId+nodeType] = true
	m.mu.Unlock()
}

// 为 sessionId 解绑逻辑节点时更新统计。update member weight
func (m *localDiscovery) onUnbindLogicNode(sessionId string, nodeType string, nodeId string) {
	m.mu.Lock()
	if nodeMap, ok := m.nodeTable[nodeId]; ok {
		delete(nodeMap, sessionId+nodeType)
		if len(nodeMap) == 0 {
			delete(m.nodeTable, nodeId)
		}
	}
	m.mu.Unlock()
}

// 获取nodeId上的session数量。get member weight
func (m *localDiscovery) getSessionCount(nodeId string) int {
	m.mu.RLock()
	nodeTypeMap, ok := m.nodeTable[nodeId]
	if !ok {
		m.mu.RUnlock()
		return 0
	}
	count := len(nodeTypeMap)
	m.mu.RUnlock()
	return count
}
