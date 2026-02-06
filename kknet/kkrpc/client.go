package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// ------------------------- rpc Client -------------------------

// ClientInvoker adapts *Client to Invoker.
type ClientInvoker struct {
	rpcClient IRpcClient
	msgRouter *kkpacket.Router
	rpcRouter *RpcRouter
}

var _ Invoker = (*ClientInvoker)(nil)
var _ IGatewayTransport = (*ClientInvoker)(nil)

func (i *ClientInvoker) Init(cli IRpcClient, rpcRouter *RpcRouter, msgRouter *kkpacket.Router) error {
	i.rpcClient = cli
	i.msgRouter = msgRouter
	i.rpcRouter = rpcRouter
	return nil
}

func (i *ClientInvoker) Invoke(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	if i.rpcClient == nil {
		return nil, ErrClientNotConnected
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
	return nil, i.rpcClient.SendBuffer(bb)
}

func (i *ClientInvoker) InvokeNoResponse(ctx context.Context, method string, data any, opts CallConfig) error {
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
	return i.rpcClient.SendBuffer(bb)
}

func (i *ClientInvoker) SendMsg(clientId kknet.CONN_ID, msg any) error {
	if i.rpcClient == nil {
		return ErrClientNotConnected
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
	return i.rpcClient.SendBuffer(stream)
}

func (i *ClientInvoker) BroadcastMsg(clientIds []kknet.CONN_ID, msg any) error {
	if i.rpcClient == nil {
		return ErrClientNotConnected
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
	return i.rpcClient.SendBuffer(stream)
}

//-------------------------------------------------------

type clientHandler struct {
	c *ClientInvoker
}

func (h *clientHandler) OnConnect(_ kknet.IConn) {

}

func (h *clientHandler) OnClose(_ kknet.IConn, _ error) {

}

func (h *clientHandler) OnRaw(_ kknet.CONN_ID, data buffers.IBuffer) {
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	kkbuffer.Put(data)
	if err != nil {
		return
	}
	var fr Frame
	if err := rpcCodec.Unmarshal(msgBytes, &fr); err != nil {
		return
	}
	kklog.Infof("client handler on raw: %d, %d, %s, %s, %d, %s", fr.T, fr.ID, fr.M, string(fr.P), fr.Code, fr.Err)
	if h.c == nil || h.c.rpcRouter == nil {
		return
	}
	switch fr.T {
	case FrameTypeResponse:
		h.c.rpcRouter.Call(context.Background(), fr.M, fr.P)
	case FrameTypeRequest:
		h.c.rpcRouter.Call(context.Background(), fr.M, fr.P)
	case FrameTypeTell:
		h.c.rpcRouter.Call(context.Background(), fr.M, fr.P)
	}
}

func (h *clientHandler) OnNoneCopy(_ kknet.CONN_ID, data []byte) {
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data)
	if err != nil {
		return
	}
	var fr Frame
	if err := rpcCodec.Unmarshal(msgBytes, &fr); err != nil {
		return
	}
	kklog.Infof("client handler on raw: %d, %d, %s, %s, %d, %s", fr.T, fr.ID, fr.M, string(fr.P), fr.Code, fr.Err)
	if h.c == nil || h.c.rpcRouter == nil {
		return
	}
	switch fr.T {
	case FrameTypeResponse:
		h.c.rpcRouter.Call(context.Background(), fr.M, fr.P)
	case FrameTypeRequest:
		h.c.rpcRouter.Call(context.Background(), fr.M, fr.P)
	case FrameTypeTell:
		h.c.rpcRouter.Call(context.Background(), fr.M, fr.P)
	}
}

func (h *clientHandler) OnMsg(_ kknet.CONN_ID, _ any, _ kkpacket.MSGID) {

}
