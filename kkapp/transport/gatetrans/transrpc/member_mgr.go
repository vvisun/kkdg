package transrpc

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
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

var _ gatetrans.IMember = (*logicMemberInfo)(nil)

func (lm *logicMemberInfo) GetNodeID() string {
	return lm.nodeId
}

func (lm *logicMemberInfo) GetNodeType() string {
	return lm.nodeType
}

type logicNodeMgr struct {
	// map[nodeId]*logicMemberInfo, key: nodeId, value: *logicMemberInfo
	logicNodeMap sync.Map
	// map[connId]nodeId, key: connId, value: nodeId
	connMap sync.Map
}

var _ gatetrans.IMemberMgr = (*logicNodeMgr)(nil)

func (slf *logicNodeMgr) Range(fn func(nodeId string, member gatetrans.IMember) bool) {
	slf.logicNodeMap.Range(func(k any, v any) bool {
		lm := v.(*logicMemberInfo)
		if lm == nil {
			return true
		}
		return fn(lm.nodeId, lm)
	})
}

func newLogicNodeMgr() *logicNodeMgr {
	return &logicNodeMgr{}
}

func (slf *logicNodeMgr) registerLogicNode(nodeId string, nodeType string, connId kknet.CONN_ID) {
	memberInfo := &logicMemberInfo{
		nodeId:   nodeId,
		nodeType: nodeType,
		connId:   connId,
	}
	kklog.Infof("[transrpc] 注册逻辑服... nodeId=%s, nodeType=%s, connId=%d", nodeId, nodeType, connId)
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
	kklog.Infof("[transrpc] 注销逻辑服... nodeId=%s", nodeId)
}

func (slf *logicNodeMgr) getLogicNode(nodeId string) *logicMemberInfo {
	value, ok := slf.logicNodeMap.Load(nodeId)
	if !ok {
		return nil
	}
	return value.(*logicMemberInfo)
}

func (slf *logicNodeMgr) getLogicNodeByConnId(connId kknet.CONN_ID) *logicMemberInfo {
	nodeId, ok := slf.connMap.Load(connId)
	if !ok {
		return nil
	}
	return slf.getLogicNode(nodeId.(string))
}
