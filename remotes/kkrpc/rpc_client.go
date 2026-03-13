package kkrpc

import (
	"sync/atomic"

	"github.com/vvisun/kkdg/kkmetrics"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkevent"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Client struct {
	cli       kknet.IClient
	pending   *pendingMap
	stopped   bool
	rpcOpts   RpcOption
	stats     RpcStats
	methodMgr *MethodManager //不用创建，从rpcRouter中传入
}

var _ IRpcClient = (*Client)(nil)
var _ ISender = (*Client)(nil)

func NewClient(addr string, opts kknet.Options, rpcRouter *rpcReceiver) *Client {
	CheckRpcOption(&rpcRouter.rpcOpts)
	cc := &Client{
		rpcOpts:   rpcRouter.rpcOpts,
		methodMgr: rpcRouter.methodMgr,
	}
	rpcRouter.stats = &cc.stats
	handler := &clientHandler{
		cli:       cc,
		rpcRouter: rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts,
		kknet.WithRawHandler(handler),
		kknet.WithStreamTool(rpcRouter.methodMgr.streamTool),
	)
	cc.cli = kktcp.NewClient(addr, handler, opts)
	cc.pending = newPendingMap(rpcRouter.rpcOpts.MaxPendingCount)
	cc.pending.stats = &cc.stats
	return cc
}

func NewClientWithCreator(opts kknet.Options, rpcRouter *rpcReceiver, cliCreator func(handler kknet.IConnLifecycleHandler, opts kknet.Options) kknet.IClient) *Client {
	CheckRpcOption(&rpcRouter.rpcOpts)
	cc := &Client{
		rpcOpts:   rpcRouter.rpcOpts,
		methodMgr: rpcRouter.methodMgr,
	}
	rpcRouter.stats = &cc.stats
	handler := &clientHandler{
		cli:       cc,
		rpcRouter: rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts,
		kknet.WithRawHandler(handler),
		kknet.WithStreamTool(rpcRouter.methodMgr.streamTool),
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
	// 订阅 rpc客户端metrics事件，通过 Stats 快照填充 MetricsEventData
	_ = kkevent.GlobalBus.Subscribe(kkmetrics.EventRpcClientMetrics, func(e *kkmetrics.MetricsEventData) {
		if e == nil {
			return
		}
		snap := c.Stats()
		e.Metrics = MetricsFromSnapshot(e.Namespace, snap)
	})
	return c.cli.Connect()
}

func (c *Client) Stop() error {
	kkevent.GlobalBus.UnsubscribeAll(kkmetrics.EventRpcClientMetrics)
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

func (c *Client) getMethodMgr() *MethodManager {
	return c.methodMgr
}

func (c *Client) Stats() RpcStatsSnapshot {
	return c.stats.Snapshot(atomic.LoadInt64(&c.pending.curPendingCount), atomic.LoadInt64(&c.pending.maxPendingCount))
}

//----------------------------------------------------------------

type clientHandler struct {
	cli       *Client
	rpcRouter *rpcReceiver
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
