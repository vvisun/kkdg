package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
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

// 同步调用
func (s *Client) Invoke(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	return nil, nil
}

// 异步调用
func (s *Client) InvokeAsync(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	return nil, nil
}

// 无响应调用
func (s *Client) InvokeNR(ctx context.Context, method string, data any, opts CallConfig) error {
	return nil
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

func (h *clientHandler) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	h.rpcRouter.OnRaw(connId, data)
}

func (h *clientHandler) OnNoneCopy(_ kknet.CONN_ID, data []byte) {

}
