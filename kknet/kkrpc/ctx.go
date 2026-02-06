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
