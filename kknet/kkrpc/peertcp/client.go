package peertcp

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkrpc"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
)

type Client struct {
	cli    *kktcp.GnetClient
	addr   string
	opts   kknet.Options
	mu     sync.Mutex
	closed bool
}

var _ kkrpc.IRpcClient = (*Client)(nil)

func NewClient(addr string, opts kknet.Options) *Client {
	cc := &Client{
		addr: addr,
		opts: opts,
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

func (c *Client) SendBuffer(data buffers.IBuffer) error {
	return c.cli.SendBuffer(data)
}

func (c *Client) Start() error {
	return c.cli.Connect()
}

func (c *Client) Stop() error {
	return c.cli.Close()
}

//-------------------------------------------------------

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
