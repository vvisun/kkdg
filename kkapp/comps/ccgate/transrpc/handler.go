package transrpc

import "context"

type rpcHandler struct {
	trans *transportorRpc
}

func (rh *rpcHandler) onRegister(ctx context.Context, msg *RpcMsgRegister) error {
	rh.trans.registerLogicNode(msg.NodeId, msg.NodeType, nil)
	return nil
}

func (rh *rpcHandler) onS2C(ctx context.Context, msg *RpcS2C) error {
	rh.trans.ForwardToClient(msg.clientId, msg.payload)
	return nil
}
