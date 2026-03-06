package transrpc

import "context"

type rpcHandler struct {
	trans *transportorRpc
}

func (rh *rpcHandler) onRegister(ctx context.Context, msg *RpcMsgRegister) error {
	rh.trans.registerLogicNode(msg.NodeId, msg.NodeType, nil)
	return nil
}

func (rh *rpcHandler) onS2C(ctx context.Context, msg *RpcS2Client) error {
	rh.trans.ForwardToClient(msg.clientId, msg.payload)
	return nil
}

func (rh *rpcHandler) onS2Clients(ctx context.Context, msg *RpcS2Clients) error {
	// rh.trans.ForwardToClients(msg.clientIds, msg.payload)
	return nil
}

func (rh *rpcHandler) onC2S(ctx context.Context, msg *RpcC2S) error {
	// rh.trans.ForwardToLogic(msg.clientId, msg.payload, msg.logicNodeId)
	return nil
}
