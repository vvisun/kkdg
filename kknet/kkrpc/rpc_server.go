package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Server struct {
	tcp       *kktcp.Server
	msgRouter *kkpacket.MsgRouter
	rpcRouter *RpcRouter
}

func NewServer(addr string, opts kknet.Options) *Server {
	s := &Server{}
	handler := &serverHandler{
		msgRouter: s.msgRouter,
		rpcRouter: s.rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts, kknet.WithRawHandler(handler))
	s.tcp = kktcp.NewServer(addr, handler, opts)
	return s
}

func (s *Server) SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error {
	return s.tcp.SendBuffer(connId, data)
}

func (s *Server) Start() error {
	return s.tcp.Start()
}

func (s *Server) Stop() error {
	return s.tcp.Stop()
}

// 同步调用
func (s *Server) Invoke(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	return nil, nil
}

// 异步调用
func (s *Server) InvokeAsync(ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	return nil, nil
}

// 无响应调用
func (s *Server) InvokeNR(ctx context.Context, method string, data any, opts CallConfig) error {
	return nil
}

//----------------------------------------------------------------

type serverHandler struct {
	msgRouter *kkpacket.MsgRouter
	rpcRouter *RpcRouter
}

func (h *serverHandler) OnConnect(conn kknet.IConn) {

}

func (h *serverHandler) OnClose(conn kknet.IConn, _ error) {

}

func (h *serverHandler) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	h.rpcRouter.OnRaw(connId, data)
}

func (h *serverHandler) OnNoneCopy(connId kknet.CONN_ID, data []byte) {

}
