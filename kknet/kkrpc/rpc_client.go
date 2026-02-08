package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Client struct {
	cli *kktcp.GnetClient
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

// 同步调用（阻塞等待结果）
func (c *Client) Invoke(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	bb, err := EncodeRpcFrameEx(FrameTypeRequest, genReqId(), method, data)
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return nil, err
	}
	err = c.cli.SendBuffer(bb)
	if err != nil {
		return nil, err
	}
	return nil, nil
}

// 异步调用（非阻塞等待结果）
func (c *Client) InvokeAsync(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	return nil, nil
}

// 无响应调用（没有结果，单向调用）
func (c *Client) InvokeNR(ctx context.Context, method string, data any, opts CallConfig) error {
	return nil
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
	bb := h.rpcRouter.OnRaw(connId, data)
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
