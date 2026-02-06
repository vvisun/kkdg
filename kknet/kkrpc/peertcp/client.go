package peertcp

import (
	"sync"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Client struct {
	cli    *kktcp.GnetClient
	mu     sync.Mutex
	closed bool
}

func NewClient(addr string, opts kknet.Options) *Client {
	cc := &Client{}
	handler := &clientHandler{c: cc}
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

//-------------------------------------------------------

type clientHandler struct {
	c *Client
}

func (h *clientHandler) OnConnect(_ kknet.IConn) {

}

func (h *clientHandler) OnClose(_ kknet.IConn, _ error) {

}

func (h *clientHandler) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	kkbuffer.Put(data)
}

func (h *clientHandler) OnNoneCopy(connId kknet.CONN_ID, data []byte) {

}

func (h *clientHandler) OnMsg(connId kknet.CONN_ID, msg any, msgId kkpacket.MSGID) {

}
