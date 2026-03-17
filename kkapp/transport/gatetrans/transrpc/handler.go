package transrpc

import (
	"context"

	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kknet"
)

type rpcHandler struct {
	trans *transportorRpc
}

func (rh *rpcHandler) onRegister(ctx context.Context, msg *ptotrans.RpcMsgRegister, connId kknet.CONN_ID) error {
	rh.trans.logicNodeMgr.registerLogicNode(msg.NodeId, msg.NodeType, connId)
	return nil
}

func (rh *rpcHandler) onS2C(ctx context.Context, msg *ptotrans.RpcS2Client, connId kknet.CONN_ID) error {
	rh.trans.ForwardToClient(msg.ClientId, msg.Payload)
	return nil
}

func (rh *rpcHandler) onS2Clients(ctx context.Context, msg *ptotrans.RpcS2Clients, connId kknet.CONN_ID) error {
	for _, clientId := range msg.ClientIds {
		rh.trans.ForwardToClient(clientId, msg.Payload)
	}
	return nil
}

func (rh *rpcHandler) onC2S(ctx context.Context, msg *ptotrans.RpcC2S, connId kknet.CONN_ID) error {
	logicNode := rh.trans.logicNodeMgr.getLogicNodeByConnId(connId)
	if logicNode == nil {
		return ErrLogicNodeNotRegistered //逻辑节点未注册
	}
	rh.trans.ForwardToLogic(msg.ClientId, msg.Payload, logicNode.nodeId)
	return nil
}

func (rh *rpcHandler) onClientLoginLogout(ctx context.Context, msg *ptotrans.RpcClientLoginLogout, connId kknet.CONN_ID) error {
	rh.trans.msgHooker.Notify(ptotrans.MsgIDRpcClientLoginLogout, msg)
	return nil
}

func (rh *rpcHandler) onUnregister(ctx context.Context, msg *ptotrans.RpcUnregister, connId kknet.CONN_ID) error {
	rh.trans.logicNodeMgr.unregisterLogicNode(msg.NodeId)
	return nil
}
