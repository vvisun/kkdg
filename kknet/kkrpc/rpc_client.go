package kkrpc

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Client struct {
	cli     kknet.IClient
	pending *pendingMap
}

var _ IRpcClient = (*Client)(nil)
var _ ISender = (*Client)(nil)

func NewClient(addr string, opts kknet.Options, rpcRouter *RpcReceiver) *Client {
	cc := &Client{}
	handler := &clientHandler{
		cli:       cc,
		rpcRouter: rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts, kknet.WithRawHandler(handler))
	cc.cli = kktcp.NewClient(addr, handler, opts)
	cc.pending = newPendingMap()
	return cc
}

func NewClientWithCreator(opts kknet.Options, rpcRouter *RpcReceiver, cliCreator func(handler kknet.IConnLifecycleHandler, opts kknet.Options) kknet.IClient) *Client {
	cc := &Client{}
	handler := &clientHandler{
		cli:       cc,
		rpcRouter: rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts, kknet.WithRawHandler(handler))
	cc.cli = cliCreator(handler, opts)
	cc.pending = newPendingMap()
	return cc
}

func (c *Client) SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error {
	return c.cli.SendBuffer(data)
}

func (c *Client) Start() error {
	return c.cli.Connect()
}

func (c *Client) Stop() error {
	c.pending.closeAll()
	return c.cli.Close()
}

func (c *Client) getPending() *pendingMap {
	return c.pending
}

//----------------------------------------------------------------

type clientHandler struct {
	cli       *Client
	rpcRouter *RpcReceiver
}

func (h *clientHandler) OnConnect(_ kknet.IConn) {

}

func (h *clientHandler) OnClose(_ kknet.IConn, _ error) {

}

func (h *clientHandler) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	bb := h.rpcRouter.OnRaw(connId, data, h.cli.pending)
	if bb != nil {
		if h.cli == nil {
			kkbuffer.Put(bb)
			return
		}
		h.cli.SendBuffer(0, bb)
	}
}

func (h *clientHandler) OnNoneCopy(_ kknet.CONN_ID, data []byte) {

}
