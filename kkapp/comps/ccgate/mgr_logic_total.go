package ccgate

import "sync"

type session2nodeType map[string]string

var gLogicTotalMgr = newLogicTotalManager()

// 实时统计每个逻辑节点上的会话数量，用于负载均衡。
//
// 注意：
//   - 这里统计的是“网关视角下，仍绑定在某逻辑节点上的会话”；
//   - 它不是纯粹的 TCP 在线连接数，也不会因为网关侧连接断开就立刻减少；
//   - 是否解绑，依赖上层玩家管理确认玩家已从目标逻辑服安全移除后，再通知网关摘除；
//   - 这样可以避免玩家在原逻辑服尚未清理完成时，被网关重新分配到另一个逻辑服，造成双登和数据混乱。
type logicTotalManager struct {
	logicNodeTable   map[string]session2nodeType // map[nodeId]session2nodeType
	logicNodeTableMu sync.RWMutex
}

func newLogicTotalManager() *logicTotalManager {
	return &logicTotalManager{
		logicNodeTable: make(map[string]session2nodeType),
	}
}

// 为 sessionId 绑定逻辑节点时更新统计。
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

// 为 sessionId 解绑逻辑节点时更新统计。
// 调用方应确保上层玩家管理已经确认该玩家可安全从该逻辑服摘除。
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
