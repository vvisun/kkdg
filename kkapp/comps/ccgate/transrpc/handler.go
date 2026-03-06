package transrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
)

type rpcHandler struct {
	trans *transportorRpc
}

func (rh *rpcHandler) onRegister(ctx context.Context, msg *RpcMsgRegister, connId kknet.CONN_ID) error {
	rh.trans.registerLogicNode(msg.NodeId, msg.NodeType, nil)
	return nil
}

func (rh *rpcHandler) onS2C(ctx context.Context, msg *RpcS2Client, connId kknet.CONN_ID) error {
	rh.trans.ForwardToClient(msg.clientId, msg.payload)
	return nil
}

func (rh *rpcHandler) onS2Clients(ctx context.Context, msg *RpcS2Clients, connId kknet.CONN_ID) error {
	// rh.trans.ForwardToClients(msg.clientIds, msg.payload)
	return nil
}

func (rh *rpcHandler) onC2S(ctx context.Context, msg *RpcC2S, connId kknet.CONN_ID) error {
	// rh.trans.ForwardToLogic(msg.clientId, msg.payload, msg.logicNodeId)
	return nil
}
