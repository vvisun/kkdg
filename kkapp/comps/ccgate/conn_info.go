package ccgate

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

type logicNodeInfo struct {
	nodeID   string
	nodeType string
	address  string
}

type clientInfo struct {
	connID     kknet.CONN_ID
	Uid        int64
	logicNodes map[string]logicNodeInfo
}

type clientManager struct {
	mu      sync.RWMutex
	clients map[kknet.CONN_ID]clientInfo
}

func newClientManager() *clientManager {
	return &clientManager{
		clients: make(map[kknet.CONN_ID]clientInfo),
	}
}

func (m *clientManager) onConnect(connID kknet.CONN_ID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clients[connID] = clientInfo{
		connID:     connID,
		Uid:        0,
		logicNodes: make(map[string]logicNodeInfo),
	}
}

func (m *clientManager) onDisconnect(connID kknet.CONN_ID) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.clients, connID)
}

func (m *clientManager) onLogin(connID kknet.CONN_ID, uid int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	client, ok := m.clients[connID]
	if !ok {
		kklog.Warnf("[ccgate] client not found: connID=%d", connID)
		return
	}
	client.Uid = uid
}

func (m *clientManager) bindLogicNode(connID kknet.CONN_ID, nodeID string, nodeType string, address string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	client, ok := m.clients[connID]
	if !ok {
		kklog.Warnf("[ccgate] client not found: connID=%d", connID)
		return
	}
	client.logicNodes[nodeID] = logicNodeInfo{
		nodeID:   nodeID,
		nodeType: nodeType,
		address:  address,
	}
}

func (m *clientManager) unbindLogicNode(connID kknet.CONN_ID, nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	client, ok := m.clients[connID]
	if !ok {
		kklog.Warnf("[ccgate] client not found: connID=%d", connID)
		return
	}
	delete(client.logicNodes, nodeID)
}

func (m *clientManager) getLogicNode(connID kknet.CONN_ID, nodeType string) (*logicNodeInfo, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	client, ok := m.clients[connID]
	if !ok {
		return nil, false
	}
	for _, logicNode := range client.logicNodes {
		if logicNode.nodeType == nodeType {
			return &logicNode, true
		}
	}
	return nil, false
}
