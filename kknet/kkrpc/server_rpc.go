package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kkoption"
)

type Server struct {
	tcp       *kktcp.Server
	msgRouter *kkpacket.Router
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

//----------------------------------------------------------------

type serverHandler struct {
	msgRouter *kkpacket.Router
	rpcRouter *RpcRouter
}

func (h *serverHandler) OnConnect(_ kknet.IConn) {

}

func (h *serverHandler) OnClose(_ kknet.IConn, _ error) {

}

func (h *serverHandler) OnRaw(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	kkbuffer.Put(data)
	if err != nil {
		return
	}
	var fr Frame
	if err := rpcCodec.Unmarshal(msgBytes, &fr); err != nil {
		return
	}
	kklog.Infof("server handler on raw: %d, %d, %s, %s, %d, %s", fr.T, fr.ID, fr.M, string(fr.P), fr.Code, fr.Err)
	if h.rpcRouter == nil {
		return
	}
	switch fr.T {
	case FrameTypeRequest:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	case FrameTypeResponse:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	case FrameTypeTell:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	}
}

func (h *serverHandler) OnNoneCopy(_ kknet.CONN_ID, data []byte) {
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data)
	if err != nil {
		return
	}
	var fr Frame
	if err := rpcCodec.Unmarshal(msgBytes, &fr); err != nil {
		return
	}
	kklog.Infof("server handler on raw: %d, %d, %s, %s, %d, %s", fr.T, fr.ID, fr.M, string(fr.P), fr.Code, fr.Err)
	if h.rpcRouter == nil {
		return
	}
	switch fr.T {
	case FrameTypeRequest:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	case FrameTypeResponse:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	case FrameTypeTell:
		h.rpcRouter.OnMsg(context.Background(), fr.M, fr.P)
	}
}

func (h *serverHandler) OnMsg(_ kknet.CONN_ID, _ any, _ kkpacket.MSGID) {

}
