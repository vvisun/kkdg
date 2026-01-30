package gnrpc

import (
	"context"
	"errors"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

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
