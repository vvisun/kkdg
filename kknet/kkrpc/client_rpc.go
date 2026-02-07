package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Client struct {
	cli       *kktcp.GnetClient
	msgRouter *kkpacket.MsgRouter
	rpcRouter *RpcRouter
}

func NewClient(addr string, opts kknet.Options) *Client {
	cc := &Client{}
	handler := &clientHandler{
		msgRouter: cc.msgRouter,
		rpcRouter: cc.rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts, kknet.WithRawHandler(handler))
	cc.cli = kktcp.NewClient(addr, handler, opts)
	return cc
}

func (c *Client) SendBuffer(data *kkbuffer.ByteBuffer) error {
	return c.cli.SendBuffer(data)
}

func (c *Client) Start() error {
	return c.cli.Connect()
}

func (c *Client) Stop() error {
	return c.cli.Close()
}

//----------------------------------------------------------------

type clientHandler struct {
	msgRouter *kkpacket.MsgRouter
	rpcRouter *RpcRouter
}

func (h *clientHandler) OnConnect(_ kknet.IConn) {

}

func (h *clientHandler) OnClose(_ kknet.IConn, _ error) {

}

func (h *clientHandler) OnRaw(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		kkbuffer.Put(data)
		return
	}
	var fr Frame
	if err := rpcCodec.Unmarshal(msgBytes, &fr); err != nil {
		kkbuffer.Put(data)
		return
	}
	kkbuffer.Put(data)
	kklog.Infof("client handler on raw: %d, %d, %s, %s, %d, %s", fr.T, fr.ID, fr.M, string(fr.P), fr.Code, fr.Err)
	if h.rpcRouter == nil {
		return
	}
	switch fr.T {
	case FrameTypeResponse:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	case FrameTypeRequest:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	case FrameTypeTell:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
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
	if h.rpcRouter == nil {
		return
	}
	switch fr.T {
	case FrameTypeResponse:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	case FrameTypeRequest:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	case FrameTypeTell:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	}
}

func (h *clientHandler) OnMsg(_ kknet.CONN_ID, _ any, _ kkpacket.MSGID) {

}
