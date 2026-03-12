package kkrpc

import (
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Client struct {
	cli     kknet.IClient
	pending *pendingMap
	stopped bool
	rpcOpts RpcOption
	stats   RpcStats
}

var _ IRpcClient = (*Client)(nil)
var _ ISender = (*Client)(nil)

func NewClient(addr string, opts kknet.Options, rpcRouter *RpcReceiver) *Client {
	CheckRpcOption(&rpcRouter.rpcOpts)
	cc := &Client{
		rpcOpts: rpcRouter.rpcOpts,
	}
	rpcRouter.stats = &cc.stats
	handler := &clientHandler{
		cli:       cc,
		rpcRouter: rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts,
		kknet.WithRawHandler(handler),
		kknet.WithStreamTool(rpcRouter.rpcOpts.StreamTool),
	)
	cc.cli = kktcp.NewClient(addr, handler, opts)
	cc.pending = newPendingMap(rpcRouter.rpcOpts.MaxPendingCount)
	cc.pending.stats = &cc.stats
	return cc
}

func NewClientWithCreator(opts kknet.Options, rpcRouter *RpcReceiver, cliCreator func(handler kknet.IConnLifecycleHandler, opts kknet.Options) kknet.IClient) *Client {
	CheckRpcOption(&rpcRouter.rpcOpts)
	cc := &Client{
		rpcOpts: rpcRouter.rpcOpts,
	}
	rpcRouter.stats = &cc.stats
	handler := &clientHandler{
		cli:       cc,
		rpcRouter: rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts,
		kknet.WithRawHandler(handler),
		kknet.WithStreamTool(rpcRouter.rpcOpts.StreamTool),
	)
	cc.cli = cliCreator(handler, opts)
	cc.pending = newPendingMap(rpcRouter.rpcOpts.MaxPendingCount)
	cc.pending.stats = &cc.stats
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
	err := c.cli.Close()
	if err == nil {
		c.stopped = true
	}
	return err
}

func (c *Client) IsStopped() bool {
	return c.stopped
}

func (c *Client) getPending() *pendingMap {
	return c.pending
}

func (c *Client) getStreamTool() kkpacket.IPacket {
	return c.rpcOpts.StreamTool
}

func (c *Client) getFrameCodec() kkcodec.ICodec {
	return c.rpcOpts.FrameCodec
}

func (c *Client) getPayloadCodec() kkcodec.ICodec {
	return c.rpcOpts.PayloadCodec
}

func (c *Client) Stats() RpcStatsSnapshot {
	return c.stats.Snapshot(atomic.LoadInt64(&c.pending.curPendingCount), atomic.LoadInt64(&c.pending.maxPendingCount))
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
