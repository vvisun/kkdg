package kkrpc

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Client struct {
	cli     *kktcp.GnetClient
	pending *pendingMap
}

var _ IRpcClient = (*Client)(nil)

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
	cli       *Client
	rpcRouter *RpcReceiver
}

func (h *clientHandler) OnConnect(_ kknet.IConn) {

}

func (h *clientHandler) OnClose(_ kknet.IConn, _ error) {

}

func (h *clientHandler) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	bb := h.rpcRouter.OnRaw(connId, data, h.cli.pending.cbMap)
	if bb != nil {
		if h.cli == nil {
			kkbuffer.Put(bb)
			return
		}
		h.cli.SendBuffer(bb)
	}
}

func (h *clientHandler) OnNoneCopy(_ kknet.CONN_ID, data []byte) {

}
