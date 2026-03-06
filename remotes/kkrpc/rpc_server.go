package kkrpc

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Server struct {
	tcp              kknet.IServer
	pending          *pendingMap
	lifeCycleHandler kknet.IConnLifecycleHandler
}

var _ IRpcServer = (*Server)(nil)
var _ ISender = (*Server)(nil)

func NewServer(addr string, opts kknet.Options, rpcRouter *RpcReceiver) *Server {
	s := &Server{}
	handler := &serverHandler{
		svr:       s,
		rpcRouter: rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts, kknet.WithRawHandler(handler))
	s.tcp = kktcp.NewServer(addr, handler, opts)
	s.pending = newPendingMap()
	return s
}

func NewServerWithCreator(opts kknet.Options, rpcRouter *RpcReceiver, svrCreator func(handler kknet.IConnLifecycleHandler, opts kknet.Options) kknet.IServer) *Server {
	s := &Server{}
	handler := &serverHandler{
		svr:       s,
		rpcRouter: rpcRouter,
	}
	kkoption.ApplyOptionsTo(&opts, kknet.WithRawHandler(handler))
	s.tcp = svrCreator(handler, opts)
	s.pending = newPendingMap()
	return s
}

func (s *Server) SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error {
	return s.tcp.SendBuffer(connId, data)
}

func (s *Server) Start() error {
	return s.tcp.Start()
}

func (s *Server) Stop() error {
	s.pending.closeAll()
	return s.tcp.Stop()
}

func (s *Server) GetConnManager() kknet.IConnManager {
	return s.tcp.GetConnManager()
}

func (s *Server) getPending() *pendingMap {
	return s.pending
}

func (s *Server) SetLifeCycleHandler(handler kknet.IConnLifecycleHandler) {
	s.lifeCycleHandler = handler
}

//----------------------------------------------------------------

type serverHandler struct {
	svr       *Server
	rpcRouter *RpcReceiver
}

func (h *serverHandler) OnConnect(conn kknet.IConn) {
	if h.svr.lifeCycleHandler != nil {
		h.svr.lifeCycleHandler.OnConnect(conn)
	}
}

func (h *serverHandler) OnClose(conn kknet.IConn, err error) {
	if h.svr.lifeCycleHandler != nil {
		h.svr.lifeCycleHandler.OnClose(conn, err)
	}
}

func (h *serverHandler) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	bb := h.rpcRouter.OnRaw(connId, data, h.svr.pending)
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
