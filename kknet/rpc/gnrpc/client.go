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
	}
	cl.cli = kktcp.NewClient(addr, &clientHandler{c: cl}, opts...)
	return cl
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

// Invoke performs a unary RPC call.
// req is raw payload bytes; resp is raw payload bytes.
func (c *Client) Invoke(ctx context.Context, method string, req []byte) ([]byte, error) {
	if method == "" {
		return nil, ErrInvalidFrame
	}
	if ctx == nil {
		ctx = context.Background()
	}

	inv := chainClientInterceptors(c.clientInterceptors, c.invokeRaw)
	return inv(ctx, method, req)
}

func (c *Client) invokeRaw(ctx context.Context, method string, req []byte) ([]byte, error) {
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
	if fr.T != FrameTypeResponse || fr.ID == 0 {
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

