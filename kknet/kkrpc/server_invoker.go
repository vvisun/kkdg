package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// ------------------------- rpc Server -------------------------

// ConnInvoker adapts (Server + ConnID) to Invoker, enabling server-initiated calls to a connected peer.
type ConnInvoker struct {
	connId    kknet.CONN_ID
	rpcServer IRpcServer
	msgRouter *kkpacket.MsgRouter
	rpcRouter *RpcRouter
}

var _ Invoker = (*ConnInvoker)(nil)
var _ IGatewayTransport = (*ConnInvoker)(nil)

func (i *ConnInvoker) Init(server IRpcServer, connId kknet.CONN_ID, rpcRouter *RpcRouter, msgRouter *kkpacket.MsgRouter) error {
	i.rpcServer = server
	i.connId = connId
	i.msgRouter = msgRouter
	i.rpcRouter = rpcRouter
	return nil
}

func (i *ConnInvoker) Invoke(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	if i.rpcServer == nil {
		return nil, ErrServerNotStarted
	}
	// encode data
	rawData, err := dataCodec.Marshal(data)
	if err != nil {
		return nil, err
	}
	// encode frame
	request := Frame{
		T:  FrameTypeRequest,
		ID: genReqId(),
		M:  method,
		P:  rawData,
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	if err != nil {
		return nil, err
	}
	// encode stream
	bb, err := kkpacket.DefaultStreamPacket().Pack(rawRequest)
	if err != nil {
		return nil, err
	}
	// send buffer
	return nil, i.rpcServer.SendBuffer(i.connId, bb)
}

func (i *ConnInvoker) InvokeNoResponse(ctx context.Context, method string, data any, opts CallConfig) error {
	if i.rpcServer == nil {
		return ErrServerNotStarted
	}
	// encode data
	rawData, err := dataCodec.Marshal(data)
	if err != nil {
		return err
	}
	// encode frame
	request := Frame{
		T:  FrameTypeTell,
		ID: 0,
		M:  method,
		P:  rawData,
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	if err != nil {
		return err
	}
	// encode stream
	bb, err := kkpacket.DefaultStreamPacket().Pack(rawRequest)
	if err != nil {
		return err
	}
	// send buffer
	return i.rpcServer.SendBuffer(i.connId, bb)
}

func (i *ConnInvoker) SendMsg(clientId kknet.CONN_ID, msg any) error {
	if i.rpcServer == nil {
		return ErrServerNotStarted
	}

	// encode message
	msgBB, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), i.msgRouter)
	if err != nil {
		kkbuffer.Put(msgBB)
		return err
	}
	request := TransMsg{
		ClientId: clientId,
		Data:     msgBB.Bytes(),
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	kkbuffer.Put(msgBB)
	if err != nil {
		return err
	}

	// encode frame
	requestFrame := Frame{
		T:  FrameTypeTransMsg,
		ID: 0,
		M:  "TransMsg",
		P:  rawRequest,
	}
	rawRequestFrame, err := rpcCodec.Marshal(&requestFrame)
	if err != nil {
		return err
	}
	stream, err := kkpacket.DefaultStreamPacket().Pack(rawRequestFrame)
	if err != nil {
		return err
	}

	// send buffer
	return i.rpcServer.SendBuffer(i.connId, stream)
}

func (i *ConnInvoker) BroadcastMsg(clientIds []kknet.CONN_ID, msg any) error {
	if len(clientIds) == 0 {
		return nil
	}
	if i.rpcServer == nil {
		return ErrServerNotStarted
	}

	// encode message
	msgBB, err := kkpacket.EncodeStream(msg, kkpacket.DefaultStreamPacket(), i.msgRouter)
	if err != nil {
		kkbuffer.Put(msgBB)
		return err
	}
	request := TransBroadMsg{
		Clients: clientIds,
		Data:    msgBB.Bytes(),
	}
	rawRequest, err := rpcCodec.Marshal(&request)
	kkbuffer.Put(msgBB)
	if err != nil {
		return err
	}

	// encode frame
	requestFrame := Frame{
		T:  FrameTypeTransBroadMsg,
		ID: 0,
		M:  "TransBroadMsg",
		P:  rawRequest,
	}
	rawRequestFrame, err := rpcCodec.Marshal(&requestFrame)
	if err != nil {
		return err
	}
	stream, err := kkpacket.DefaultStreamPacket().Pack(rawRequestFrame)
	if err != nil {
		return err
	}

	// send buffer
	return i.rpcServer.SendBuffer(i.connId, stream)
}
