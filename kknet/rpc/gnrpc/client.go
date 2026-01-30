package gnrpc

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
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
	cl.cli = kktcp.NewClient(addr, &clientHandler{c: cl}, opts...)
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

// InvokeNoResponse performs a unary RPC call without waiting for response.
// It is a fire-and-forget oneway request.
func (c *Client) InvokeNoResponse(ctx context.Context, method string, req []byte, opts ...CallOption) error {
	if method == "" {
		return ErrInvalidFrame
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cfg := applyCallOptions(opts)
	var cancel context.CancelFunc
	ctx, cancel = maybeApplyTimeout(ctx, cfg.timeout)
	defer cancel()

	inv := chainClientInterceptors(c.clientInterceptors, func(ctx context.Context, method string, req []byte) ([]byte, error) {
		return nil, c.invokeOneway(ctx, method, req, cfg)
	})
	_, err := inv(ctx, method, req)
	return err
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

// Invoke performs a unary RPC call.
// req is raw payload bytes; resp is raw payload bytes.
func (c *Client) Invoke(ctx context.Context, method string, req []byte, opts ...CallOption) ([]byte, error) {
	if method == "" {
		return nil, ErrInvalidFrame
	}
	if ctx == nil {
		ctx = context.Background()
	}

	cfg := applyCallOptions(opts)
	var cancel context.CancelFunc
	ctx, cancel = maybeApplyTimeout(ctx, cfg.timeout)
	defer cancel()

	inv := chainClientInterceptors(c.clientInterceptors, func(ctx context.Context, method string, req []byte) ([]byte, error) {
		return c.invokeRaw(ctx, method, req, cfg)
	})
	return inv(ctx, method, req)
}

func (c *Client) invokeRaw(ctx context.Context, method string, req []byte, cfg callConfig) ([]byte, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClientClosed
	}
	c.mu.Unlock()

	id := c.seq.Add(1)
	if id == 0 {
		id = c.seq.Add(1)
	}

	ch := make(chan Frame, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClientClosed
	}
	c.pending[id] = ch
	c.mu.Unlock()

	fr := Frame{
		T:  FrameTypeRequest,
		ID: id,
		M:  method,
		DL: ctxDeadlineUnixMs(ctx),
		P:  req,
		H:  cfg.headers,
	}
	b, err := c.codec.Marshal(&fr)
	if err != nil {
		c.delPending(id)
		return nil, err
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(b)
	if err != nil {
		c.delPending(id)
		return nil, err
	}
	if err := c.cli.SendBuffer(bb); err != nil {
		kkbuffer.Put(bb)
		c.delPending(id)
		if errors.Is(err, kkerrors.ErrClientNotConnected) {
			return nil, ErrClientNotConnected
		}
		return nil, err
	}

	select {
	case resp, ok := <-ch:
		c.delPending(id)
		if !ok {
			return nil, ErrClientClosed
		}
		if resp.T != FrameTypeResponse || resp.ID != id {
			return nil, ErrInvalidFrame
		}
		// capture response metadata if requested
		if cfg.respHeaders != nil {
			if resp.RH == nil {
				*cfg.respHeaders = nil
			} else {
				md := make(MD, len(resp.RH))
				for k, v := range resp.RH {
					md[k] = v
				}
				*cfg.respHeaders = md
			}
		}
		if cfg.respTrailers != nil {
			if resp.RT == nil {
				*cfg.respTrailers = nil
			} else {
				md := make(MD, len(resp.RT))
				for k, v := range resp.RT {
					md[k] = v
				}
				*cfg.respTrailers = md
			}
		}
		if resp.Code != 0 {
			return nil, Status(Code(resp.Code), resp.Err)
		}
		return resp.P, nil
	case <-ctx.Done():
		c.delPending(id)
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		// safety net; prefer using ctx with deadline/timeout.
		c.delPending(id)
		return nil, context.DeadlineExceeded
	}
}

func (c *Client) delPending(id uint64) {
	c.mu.Lock()
	ch := c.pending[id]
	delete(c.pending, id)
	c.mu.Unlock()
	_ = ch
}

func (c *Client) invokeOneway(ctx context.Context, method string, req []byte, cfg callConfig) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClientClosed
	}
	c.mu.Unlock()

	fr := Frame{
		T:  FrameTypeOneway,
		ID: 0,
		M:  method,
		DL: ctxDeadlineUnixMs(ctx),
		P:  req,
		H:  cfg.headers,
	}
	b, err := c.codec.Marshal(&fr)
	if err != nil {
		return err
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(b)
	if err != nil {
		return err
	}
	if err := c.cli.SendBuffer(bb); err != nil {
		kkbuffer.Put(bb)
		if errors.Is(err, kkerrors.ErrClientNotConnected) {
			return ErrClientNotConnected
		}
		return err
	}
	return nil
}

type clientHandler struct {
	c *Client
}

func (h *clientHandler) OnConnect(_ kknet.IConn) {}

func (h *clientHandler) OnMessage(_ kknet.IConn, data buffers.IBuffer) {
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

