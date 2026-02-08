package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Server struct {
	tcp *kktcp.Server
}

var _ IRpcServer = (*Server)(nil)

func NewServer(addr string, opts kknet.Options, rpcRouter *RpcReceiver) *Server {
	s := &Server{}
	handler := &serverHandler{
		svr:       s,
		rpcRouter: rpcRouter,
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

// 同步调用（阻塞等待结果）
func (s *Server) Invoke(connID kknet.CONN_ID, ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	conn := s.tcp.GetConnManager().GetConn(int64(connID))
	if conn == nil {
		return nil, kkerrors.ErrConnNotFound
	}
	return nil, nil
}

// 异步调用（非阻塞等待结果）
func (s *Server) InvokeAsync(connID kknet.CONN_ID, ctx context.Context, method string, data any, opts CallConfig) (any, error) {
	conn := s.tcp.GetConnManager().GetConn(int64(connID))
	if conn == nil {
		return nil, kkerrors.ErrConnNotFound
	}
	return nil, nil
}

// 无响应调用（没有结果，单向调用）
func (s *Server) InvokeNR(connID kknet.CONN_ID, ctx context.Context, method string, data any, opts CallConfig) error {
	conn := s.tcp.GetConnManager().GetConn(int64(connID))
	if conn == nil {
		return kkerrors.ErrConnNotFound
	}
	return nil
}

//----------------------------------------------------------------

type serverHandler struct {
	svr       *Server
	rpcRouter *RpcReceiver
}

func (h *serverHandler) OnConnect(conn kknet.IConn) {

}

func (h *serverHandler) OnClose(conn kknet.IConn, _ error) {

}

func (h *serverHandler) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	bb := h.rpcRouter.OnRaw(connId, data)
	if bb != nil {
		if h.svr == nil {
			kkbuffer.Put(bb)
			return
		}
		h.svr.SendBuffer(connId, bb)
	}
}

func (h *serverHandler) OnNoneCopy(connId kknet.CONN_ID, data []byte) {

}
