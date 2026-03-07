package transrpc

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

type logicMemberInfo struct {
	nodeId   string
	nodeType string
	connId   kknet.CONN_ID
	// weight   int //权重，用于负载均衡
	// status   int //状态（kkdiscovery.NodeStatusOnline或kkdiscovery.NodeStatusOffline）
}

type logicNodeMgr struct {
	logicNodeMap sync.Map // map[nodeId]*logicMemberInfo
	connMap      sync.Map // map[connId]nodeId
}

func (slf *logicNodeMgr) registerLogicNode(nodeId string, nodeType string, connId kknet.CONN_ID) {
	memberInfo := &logicMemberInfo{
		nodeId:   nodeId,
		nodeType: nodeType,
		connId:   connId,
	}
	kklog.Infof("[ccgate] register logic node... nodeId=%s, nodeType=%s, connId=%d", nodeId, nodeType, connId)
	slf.logicNodeMap.Store(nodeId, memberInfo)
	slf.connMap.Store(connId, nodeId)
}

func (slf *logicNodeMgr) unregisterLogicNode(nodeId string) {
	info, ok := slf.logicNodeMap.Load(nodeId)
	if !ok {
		return
	}
	memberInfo := info.(*logicMemberInfo)
	slf.connMap.Delete(memberInfo.connId)
	slf.logicNodeMap.Delete(nodeId)
	kklog.Infof("[ccgate] unregister logic node... nodeId=%s", nodeId)
}

func (slf *logicNodeMgr) getLogicNode(nodeId string) *logicMemberInfo {
	value, ok := slf.logicNodeMap.Load(nodeId)
	if !ok {
		return nil
	}
	return value.(*logicMemberInfo)
}

func (slf *logicNodeMgr) getLogicNodeByConnId(connId kknet.CONN_ID) *logicMemberInfo {
	value, ok := slf.connMap.Load(connId)
	if !ok {
		return nil
	}
	return value.(*logicMemberInfo)
}
