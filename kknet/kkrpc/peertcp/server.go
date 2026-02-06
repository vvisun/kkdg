package peertcp

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Server struct {
	tcp *kktcp.Server
}

func NewServer(addr string, opts kknet.Options) *Server {
	s := &Server{}
	handler := &serverHandler{s: s}
	kkoption.ApplyOptionsTo(&opts, kknet.WithRawHandler(handler))
	s.tcp = kktcp.NewServer(addr, handler, opts)
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
