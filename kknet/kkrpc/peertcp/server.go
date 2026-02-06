package peertcp

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkrpc"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
)

type Server struct {
	tcp  *kktcp.Server
	addr string
	opts kknet.Options
}

var _ kkrpc.IRpcServer = (*Server)(nil)

func NewServer(addr string, opts kknet.Options) *Server {
	s := &Server{
		addr: addr,
		opts: opts,
	}
	h := &serverHandler{s: s}
	reliesOpts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithNoneCopyHandler(h),
		kknet.WithMsgHandler(h),
		kknet.WithBufferSizes(2*1024, 2*1024),
	)
	s.tcp = kktcp.NewServer(addr, h, reliesOpts)
	return s
}

func (s *Server) SendBuffer(connId kknet.CONN_ID, data buffers.IBuffer) error {
	conn := s.tcp.GetConnManager().GetConn(connId)
	if conn == nil {
		return kkerrors.ErrConnNotFound
	}
	return conn.SendBuffer(data)
}

func (s *Server) Start() error {
	return s.tcp.Start()
}

func (s *Server) Stop() error {
	return s.tcp.Stop()
}

//-------------------------------------------------------

type serverHandler struct {
	s *Server
}

func (h *serverHandler) OnConnect(_ kknet.IConn) {

}

func (h *serverHandler) OnClose(_ kknet.IConn, _ error) {

}

func (h *serverHandler) OnRaw(_ kknet.CONN_ID, data buffers.IBuffer) {

}

func (h *serverHandler) OnNoneCopy(_ kknet.CONN_ID, data []byte) {

}

func (h *serverHandler) OnMsg(_ kknet.CONN_ID, _ any, _ kkpacket.MSGID) {

}
