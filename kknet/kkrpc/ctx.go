package kkrpc

import (
	"context"
	"time"
)

func ctxDeadlineUnixMs(ctx context.Context) int64 {
	if ctx == nil {
		return 0
	}
	dl, ok := ctx.Deadline()
	if !ok {
		return 0
	}
	return dl.UnixMilli()
}

func deadlineCtx(deadlineUnixMs int64) (context.Context, context.CancelFunc) {
	if deadlineUnixMs <= 0 {
		return context.Background(), func() {}
	}
	dl := time.UnixMilli(deadlineUnixMs)
	return context.WithDeadline(context.Background(), dl)
}

func maybeApplyTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok || d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}

func contextCanceled() error { return context.Canceled }

//-------------------------------------------------------

type peerKey struct{}

func getPeerState(cctx context.Context) (*peerState, bool) {
	if cctx == nil {
		return nil, false
	}
	v := cctx.Value(peerKey{})
	if v == nil {
		return nil, false
	}
	ps, ok := v.(*peerState)
	return ps, ok
}

func withPeerState(ctx context.Context, ps *peerState) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, peerKey{}, ps)
}
