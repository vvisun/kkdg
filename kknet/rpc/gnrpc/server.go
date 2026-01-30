package gnrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// Server is a unary RPC server implemented on top of kktcp(gnet).
type Server struct {
	addr string
	opts kknet.Options

	codec  *codec
	router *router
	tcp    *kktcp.Server

	serverInterceptors []UnaryServerInterceptor
}

// NewServer creates a new RPC server.
// By default it uses msgpack for internal frame encoding.
func NewServer(addr string, opts ...kknet.Option) *Server {
	cfg := kknet.ApplyOptions(opts...)
	c, _ := newCodec(kkcodec.CodecTypeMsgpack)
	s := &Server{
		addr:   addr,
		opts:   cfg,
		codec:  c,
		router: newRouter(),
	}
	s.tcp = kktcp.NewServer(addr, &serverHandler{svr: s}, opts...)
	return s
}

// SetFrameCodec sets the codec used for encoding/decoding internal frames.
func (s *Server) SetFrameCodec(codecType uint8) error {
	c, err := newCodec(codecType)
	if err != nil {
		return err
	}
	s.codec = c
	return nil
}

// UseInterceptor adds server-side unary interceptors.
func (s *Server) UseInterceptor(interceptors ...UnaryServerInterceptor) {
	for _, it := range interceptors {
		if it != nil {
			s.serverInterceptors = append(s.serverInterceptors, it)
		}
	}
}

// Register registers a unary handler for method.
func (s *Server) Register(method string, h Handler) {
	s.router.Register(method, h)
}

func (s *Server) Start() error { return s.tcp.Start() }
func (s *Server) Stop() error  { return s.tcp.Stop() }
func (s *Server) Addr() string { return s.tcp.Addr() }
func (s *Server) Stats() kknet.StatsSnapshot {
	return s.tcp.Stats()
}
func (s *Server) GetConnManager() kknet.IConnManager {
	return s.tcp.GetConnManager()
}

type serverHandler struct {
	svr *Server
}

func (h *serverHandler) OnConnect(_ kknet.IConn) {}

func (h *serverHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}

	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		return
	}

	var fr Frame
	if err := h.svr.codec.Unmarshal(msgBytes, &fr); err != nil {
		return
	}
	if fr.T != FrameTypeRequest || fr.ID == 0 || fr.M == "" {
		return
	}

	ctx, cancel := deadlineCtx(fr.DL)
	defer cancel()
	// Attach incoming metadata (headers) to context.
	if fr.H != nil {
		ctx = NewIncomingContext(ctx, MD(fr.H))
	}
	// Prepare response metadata container.
	ctx, meta := withServerMeta(ctx)
	// Apply server interceptor chain.
	base := func(ctx context.Context, req []byte) ([]byte, error) {
		return h.svr.router.Call(ctx, fr.M, req)
	}
	chained := chainServerInterceptors(h.svr.serverInterceptors, base, fr.M)
	respPayload, callErr := chained(ctx, fr.P)

	resp := Frame{
		T:  FrameTypeResponse,
		ID: fr.ID,
		P:  respPayload,
		RH: meta.headers,
		RT: meta.trailers,
	}
	if callErr != nil {
		resp.Code = int32(CodeOf(callErr))
		resp.Err = MsgOf(callErr)
	}

	b, err := h.svr.codec.Marshal(&resp)
	if err != nil {
		return
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(b)
	if err != nil {
		return
	}
	if err := c.SendBuffer(bb); err != nil {
		kkbuffer.Put(bb)
	}
}

func (h *serverHandler) OnClose(_ kknet.IConn, _ error) {}

// For convenience: a helper to create context-aware handlers.
func UnaryHandler(fn func(ctx context.Context, req []byte) ([]byte, error)) Handler {
	return fn
}

