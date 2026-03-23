package ccgate

import (
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xcall"
	"github.com/vvisun/kkdg/zothers/internal/demo1/comps/user"
)

func (slf *gateComponent) onNewClientConn(c kknet.IConn) {
	sid := getSessionId(c.ID(), slf.GetApplication().GetNodeId())
	slf.sessionMgr.AddConn(sid, c)
	slf.clientMgr.addClient(c.ID(), sid)
}

func (slf *gateComponent) onClientConnClose(c kknet.IConn) {
	cid := c.ID()
	sid := getSessionId(cid, slf.GetApplication().GetNodeId())

	// 通知所有已绑定的逻辑服，网关处该客户端连接已断开
	bindTbl := slf.logicBindMgr.getSessionBindTable(sid)
	if bindTbl != nil {
		bindTbl.rangeLogicItems(func(nodeType string, logicItem *clientLogicItem) bool {
			if logicItem.nodeId != "" {
				logicNodeId := logicItem.nodeId
				xcall.AntsSafeGo(func() {
					if slf.transportor != nil {
						slf.transportor.NotifyClientDisconnect(sid, logicNodeId, cid)
					}
				})
			}
			return true
		})
	}

	slf.sessionMgr.RemoveConn(sid)
	slf.clientMgr.removeClient(cid)
	slf.userMgr.onSessionDisconnect(sid)
	slf.logicBindMgr.onSessionDisconnect(sid)
	slf.feedLimit.Remove(cid)
}

func (slf *gateComponent) loginHook(msg *ptotrans.RpcClientLoginLogout) {
	if msg.IsLogin {
		kklog.Debugf("[ccgate]客户端登录逻辑服成功: %#v", msg)

		slf.logicBindMgr.userBind(user.USER_ID(msg.UserId), msg.NodeType, msg.NodeId)

		kickList := slf.userMgr.onUserLogin(user.USER_ID(msg.UserId), msg.ClientId)
		if slf.errCallback != nil && len(kickList) > 0 {
			for _, kick := range kickList {
				if conn, err := slf.sessionMgr.GetConn(kick.sessionId); err == nil {
					// 从客户端管理器中移除，不再接收被踢连接的消息。
					slf.clientMgr.removeClient(conn.ID())
					// 通知业务层，用户被顶号/被踢出会话。
					slf.feedCallback(conn.ID(), ERR_USER_KICKED)
				}
			}
		}
	} else {
		kklog.Debugf("[ccgate]客户端登出逻辑服成功: %#v", msg)
		slf.logicBindMgr.userUnbind(user.USER_ID(msg.UserId), msg.NodeType)
		slf.localDis.onUnbindLogicNode(msg.ClientId, msg.NodeType, msg.NodeId)
		slf.userMgr.onUserLogout(user.USER_ID(msg.UserId))
	}
}

// 为客户端(connID)分配一个nodeType类型的逻辑节点
func (slf *gateComponent) allocLogicNode(connID kknet.CONN_ID, nodeType string) *clientLogicItem {
	if nodeType == "" {
		return nil //无效的nodeType，不分配逻辑节点
	}

	sessionID := slf.clientMgr.getSessionByConnId(connID)
	if sessionID == "" {
		return nil //客户端不存在|已被踢出会话，不分配逻辑节点
	}

	// 如果已分配，则返回已分配的逻辑节点信息
	if oldLogicItem := slf.logicBindMgr.getLogicItemBySessionId(sessionID, nodeType); oldLogicItem != nil {
		return oldLogicItem
	}

	// 选择逻辑节点
	chooseNodeId, found := slf.chooseLogicNode(nodeType)
	if !found {
		return nil //没有找到合适的逻辑节点
	}

	// 分配逻辑节点
	logicItem := slf.logicBindMgr.sessionBind(sessionID, nodeType, chooseNodeId)
	slf.localDis.onBindLogicNode(sessionID, nodeType, chooseNodeId)
	return logicItem
}

// 选择逻辑节点的唯一入口。
func (slf *gateComponent) chooseLogicNode(nodeType string) (string, bool) {
	if slf.gateOpt.TransType == transport.TransTypeShard || slf.gateOpt.TransType == transport.TransTypeRpc {
		return slf.chooseFromShardOrRpc(nodeType)
	}
	return slf.chooseFromDiscovery(nodeType)
}

// 从shard中选择权重最小的逻辑节点. return nodeId, found
func (slf *gateComponent) chooseFromShardOrRpc(nodeType string) (string, bool) {
	memberMgr := slf.transportor.(gatetrans.IMemberMgrGetter).GetMemberMgr()
	localDis := slf.localDis

	var chooseNode gatetrans.IMember = nil
	finded := false
	memberMgr.Range(func(nodeId string, member gatetrans.IMember) bool {
		if member.GetNodeType() != nodeType {
			return true
		}
		if chooseNode == nil {
			chooseNode = member
			finded = true
			return true
		}
		if localDis.getMemberWeight(member.GetNodeID()) < localDis.getMemberWeight(chooseNode.GetNodeID()) {
			chooseNode = member
			finded = true
		}
		return true
	})
	if finded {
		return chooseNode.GetNodeID(), true
	}
	return "", false
}

// 从discovery中选择权重最小的逻辑节点. return nodeId, found
func (slf *gateComponent) chooseFromDiscovery(nodeType string) (string, bool) {
	if slf.discovery == nil {
		return "", false
	}
	if slf.discovery.GetMemberMgr().CountOfType(nodeType) == 0 {
		return "", false
	}

	var chooseNode kkdiscovery.IMember = nil
	finded := false
	slf.discovery.GetMemberMgr().RangeType(nodeType, func(nodeID string, member kkdiscovery.IMember) bool {
		if chooseNode == nil {
			chooseNode = member
			finded = true
			return true
		}
		if member.GetWeight() < chooseNode.GetWeight() {
			chooseNode = member
			finded = true
		}
		return true
	})
	if finded {
		return chooseNode.GetNodeID(), true
	}
	return "", false
}
