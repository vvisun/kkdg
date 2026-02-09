package gnrpc

import (
	"context"
	"errors"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// InvokeConn calls a connected peer (client) by connID and waits for response.
func (s *Server) InvokeConn(ctx context.Context, connID kknet.CONN_ID, method string, req []byte, opts ...CallOption) ([]byte, error) {
	if s == nil || s.tcp == nil {
		return nil, ErrClientNotConnected
	}
	if method == "" {
		return nil, ErrInvalidFrame
	}

	cfg := applyCallOptions(opts)
	var cancel context.CancelFunc
	ctx, cancel = maybeApplyTimeout(ctx, cfg.timeout)
	defer cancel()

	conn := s.tcp.GetConnManager().GetConn(int64(connID))
	if conn == nil {
		return nil, kkerrors.ErrConnNotFound
	}
	ps, ok := getPeerState(conn.Context())
	if !ok || ps == nil {
		ps = newPeerState()
		conn.SetContext(withPeerState(conn.Context(), ps))
	}

	id := ps.nextID()
	ch, ok := ps.addPending(id)
	if !ok {
		return nil, ErrClientClosed
	}
	defer ps.delPending(id)

	fr := Frame{
		T:  FrameTypeRequest,
		ID: id,
		M:  method,
		DL: ctxDeadlineUnixMs(ctx),
		P:  req,
	}
	b, err := s.codec.Marshal(&fr)
	if err != nil {
		return nil, err
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(b)
	if err != nil {
		return nil, err
	}
	if err := conn.SendBuffer(bb); err != nil {
		kkbuffer.Put(bb)
		if errors.Is(err, kkerrors.ErrClientNotConnected) {
			return nil, ErrClientNotConnected
		}
		return nil, err
	}

	select {
	case resp, ok := <-ch:
		if !ok {
			return nil, ErrClientClosed
		}
		// capture response metadata if requested
		if resp.Code != 0 {
			return nil, Status(Code(resp.Code), resp.Err)
		}
		return resp.P, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(30 * time.Second):
		return nil, context.DeadlineExceeded
	}
}

// InvokeConnNoResponse performs a fire-and-forget oneway call to a connected peer (client).
func (s *Server) InvokeConnNoResponse(ctx context.Context, connID kknet.CONN_ID, method string, req []byte, opts ...CallOption) error {
	if s == nil || s.tcp == nil {
		return ErrClientNotConnected
	}
	if method == "" {
		return ErrInvalidFrame
	}

	cfg := applyCallOptions(opts)
	var cancel context.CancelFunc
	ctx, cancel = maybeApplyTimeout(ctx, cfg.timeout)
	defer cancel()

	conn := s.tcp.GetConnManager().GetConn(int64(connID))
	if conn == nil {
		return kkerrors.ErrConnNotFound
	}

	fr := Frame{
		T:  FrameTypeOneway,
		ID: 0,
		M:  method,
		DL: ctxDeadlineUnixMs(ctx),
		P:  req,
	}
	b, err := s.codec.Marshal(&fr)
	if err != nil {
		return err
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(b)
	if err != nil {
		return err
	}
	if err := conn.SendBuffer(bb); err != nil {
		kkbuffer.Put(bb)
		return err
	}
	return nil
}
