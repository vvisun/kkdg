package kknet

import (
	"context"
	"encoding/binary"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/buffers"
)

// IConn represents a network connection.
type IConn interface {
	ID() int64                      //unique connection id
	Send(data []byte) error         //send data
	Close() error                   //close connection
	RemoteAddr() string             //remote address
	Context() context.Context       //get context
	SetContext(ctx context.Context) //set context
}

// IHandler handles connection lifecycle and messages.
type IHandler interface {
	OnConnect(c IConn)
	OnMessage(c IConn, data buffers.IBuffer)
	OnClose(c IConn, err error)
}

var connIDCounter atomic.Int64

// NextConnID returns a unique connection id.
func NextConnID() int64 {
	return connIDCounter.Add(1)
}

var gByteOrder binary.ByteOrder = binary.BigEndian

func SetByteOrder(order binary.ByteOrder) {
	gByteOrder = order
}

func GetByteOrder() binary.ByteOrder {
	return gByteOrder
}
