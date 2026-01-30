package gnrpc

import (
	"context"
	"errors"
	"time"
)

// FrameType indicates request/response frame.
type FrameType uint8

const (
	FrameTypeUnknown  FrameType = 0
	FrameTypeRequest  FrameType = 1
	FrameTypeResponse FrameType = 2
)

// Frame is the wire message inside the stream packet body.
// It is encoded by the configured codec (msgpack by default).
//
// NOTE: Use short field names to reduce overhead.
type Frame struct {
	T  FrameType `json:"t" msgpack:"t"`            // type
	ID uint64    `json:"id" msgpack:"id"`          // request id
	M  string    `json:"m,omitempty" msgpack:"m"`  // method
	DL int64     `json:"dl,omitempty" msgpack:"dl"`// deadline unix ms (0 means no deadline)
	P  []byte    `json:"p,omitempty" msgpack:"p"`  // payload

	Code int32  `json:"c,omitempty" msgpack:"c"` // status code (0 ok)
	Err  string `json:"e,omitempty" msgpack:"e"` // error message
}

// Handler handles a unary RPC call.
// It receives the raw payload bytes and returns raw payload bytes.
type Handler func(ctx context.Context, req []byte) ([]byte, error)

var (
	ErrClientClosed       = errors.New("gnrpc: client closed")
	ErrClientNotConnected = errors.New("gnrpc: client not connected")
	ErrMethodNotFound     = errors.New("gnrpc: method not found")
	ErrInvalidFrame       = errors.New("gnrpc: invalid frame")
	ErrDeadlineExceeded   = context.DeadlineExceeded
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

