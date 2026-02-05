package peertcp

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkrpc"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type Client struct {
	codec  kkcodec.ICodec
	router *kkrpc.RpcRouter
	cli    *kktcp.GnetClient
	addr   string
	opts   kknet.Options

	seq    atomic.Uint64
	mu     sync.Mutex
	closed bool
}

var _ kkrpc.IRpcClient = (*Client)(nil)

func NewClient(addr string, opts kknet.Options, codec kkcodec.ICodec, router *kkrpc.RpcRouter) *Client {
	cc := &Client{
		addr:   addr,
		opts:   opts,
		codec:  codec,
		router: router,
	}
	h := &clientHandler{c: cc}
	reliesOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithNoneCopyHandler(h),
		kknet.WithMsgHandler(h),
		kknet.WithBufferSizes(2*1024, 2*1024),
	)
	cc.cli = kktcp.NewClient(addr, h, reliesOpts)
	return cc
}

func (c *Client) InvokeNoResponse(ctx context.Context, method string, data any, opts kkrpc.CallConfig) error {
	return nil
}

func (c *Client) Invoke(ctx context.Context, method string, data any, opts kkrpc.CallConfig) (any, error) {
	return nil, nil
}

type clientHandler struct {
	c *Client
}

func (h *clientHandler) OnConnect(_ kknet.IConn) {

}

func (h *clientHandler) OnClose(_ kknet.IConn, _ error) {

}

func (h *clientHandler) OnRaw(_ kknet.CONN_ID, data buffers.IBuffer) {

}

func (h *clientHandler) OnNoneCopy(_ kknet.CONN_ID, data []byte) {

}

func (h *clientHandler) OnMsg(_ kknet.CONN_ID, _ any, _ kkpacket.MSGID) {

}
