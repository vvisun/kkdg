package kknet

import (
	"context"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/buffers"
)

// Conn represents a network connection.
type Conn interface {
	ID() int64
	Send(data []byte) error
	Close() error
	RemoteAddr() string
	Context() context.Context
	SetContext(ctx context.Context)
}

// Handler handles connection lifecycle and messages.
type Handler interface {
	OnConnect(c Conn)
	OnMessage(c Conn, data buffers.IBuffer)
	OnClose(c Conn, err error)
}

var connIDCounter atomic.Int64

// NextConnID returns a unique connection id.
func NextConnID() int64 {
	return connIDCounter.Add(1)
}
