package gnrpc

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"google.golang.org/protobuf/proto"
)

// Client is a unary RPC client implemented on top of kktcp(gnet).
type Client struct {
	addr string
	opts kknet.Options

	codec *codec

	cli *kktcp.GnetClient

	seq atomic.Uint64

	mu      sync.Mutex
	pending map[uint64]chan Frame
	closed  bool

	clientInterceptors []UnaryClientInterceptor

	router *router
}

// NewClient creates a new RPC client.
// By default it uses msgpack for internal frame encoding.
func NewClient(addr string, opts ...kknet.Option) *Client {
	cfg := kknet.ApplyOptions(opts...)
	c, _ := newCodec(kkcodec.CodecTypeMsgpack)
	cl := &Client{
		addr:    addr,
		opts:    cfg,
		codec:   c,
		pending: make(map[uint64]chan Frame),
		router:  newRouter(),
	}
	h := &clientHandler{c: cl}
	// gnrpc relies on RawHandler delivery from ReadProcessor.
	cl.cli = kktcp.NewClient(addr, h, append(opts, kknet.WithRawHandler(h))...)
	return cl
}

// Register registers a unary handler for peer-initiated calls (server->client).
func (c *Client) Register(method string, h Handler) { c.router.Register(method, h) }

// RegisterProto registers a protobuf unary handler for peer-initiated calls (server->client).
func (c *Client) RegisterProto(method string, newReq func() proto.Message, handler func(ctx context.Context, req proto.Message) (proto.Message, error)) {
	if newReq == nil || handler == nil {
		return
	}
	c.Register(method, func(ctx context.Context, reqBytes []byte) ([]byte, error) {
		req := newReq()
		if req == nil {
			return nil, Status(CodeInternal, "nil request factory")
		}
		if len(reqBytes) > 0 {
			if err := proto.Unmarshal(reqBytes, req); err != nil {
				return nil, Status(CodeInvalidArgument, err.Error())
			}
		}
		resp, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, nil
		}
		b, err := proto.Marshal(resp)
		if err != nil {
			return nil, Status(CodeInternal, err.Error())
		}
		return b, nil
	})
}

// SetFrameCodec sets the codec used for encoding/decoding internal frames.
func (c *Client) SetFrameCodec(codecType uint8) error {
	cc, err := newCodec(codecType)
	if err != nil {
		return err
	}
	c.codec = cc
	return nil
}

// UseInterceptor adds client-side unary interceptors.
func (c *Client) UseInterceptor(interceptors ...UnaryClientInterceptor) {
	for _, it := range interceptors {
		if it != nil {
			c.clientInterceptors = append(c.clientInterceptors, it)
		}
	}
}

func (c *Client) Connect() error {
	return c.cli.Connect()
}

func (c *Client) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	// fail all pending
	for id, ch := range c.pending {
		delete(c.pending, id)
		close(ch)
	}
	c.mu.Unlock()
	return c.cli.Close()
}

type clientHandler struct {
	c *Client
}

func (h *clientHandler) OnConnect(_ kknet.IConn) {}

// OnRaw implements kknet.IRawHandler.
// Note: data is a framed packet: [length,message].
func (h *clientHandler) OnRaw(_ kknet.CONN_ID, data buffers.IBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		return
	}
	var fr Frame
	if err := h.c.codec.Unmarshal(msgBytes, &fr); err != nil {
		return
	}
	switch fr.T {
	case FrameTypeResponse:
		if fr.ID == 0 {
			return
		}
		h.c.mu.Lock()
		ch := h.c.pending[fr.ID]
		h.c.mu.Unlock()
		if ch == nil {
			return
		}
		select {
		case ch <- fr:
		default:
		}
	case FrameTypeRequest:
		if fr.ID == 0 || fr.M == "" {
			return
		}
		// handle peer-initiated request and respond
		ctx, cancel := deadlineCtx(fr.DL)
		defer cancel()
		if fr.H != nil {
			ctx = NewIncomingContext(ctx, MD(fr.H))
		}
		ctx, meta := withServerMeta(ctx)
		respPayload, callErr := h.c.router.Call(ctx, fr.M, fr.P)
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
		b, err := h.c.codec.Marshal(&resp)
		if err != nil {
			return
		}
		bb, err := kkpacket.DefaultStreamPacket().Pack(b)
		if err != nil {
			return
		}
		// Use underlying gnet client to send back.
		if err := h.c.cli.SendBuffer(bb); err != nil {
			kkbuffer.Put(bb)
		}
	case FrameTypeOneway:
		if fr.M == "" {
			return
		}
		ctx, cancel := deadlineCtx(fr.DL)
		defer cancel()
		if fr.H != nil {
			ctx = NewIncomingContext(ctx, MD(fr.H))
		}
		_, _ = h.c.router.Call(ctx, fr.M, fr.P)
	default:
		return
	}
}

func (h *clientHandler) OnClose(_ kknet.IConn, _ error) {
	// Fail all pending to avoid goroutine leaks.
	h.c.mu.Lock()
	for id, ch := range h.c.pending {
		delete(h.c.pending, id)
		close(ch)
	}
	h.c.mu.Unlock()
}
