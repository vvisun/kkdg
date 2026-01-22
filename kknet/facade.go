package kknet

import (
	"context"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/buffers"
)

// CONN_ID is the type of connection ID.
type CONN_ID = int64

var connIDCounter atomic.Int64 // counter for connection ID. unique id for the connection.

// NextConnID returns a unique connection ID.
func NextConnID() CONN_ID {
	return connIDCounter.Add(1)
}

// IConn represents a network connection.
type IConn interface {
	ID() CONN_ID                             //unique connection id
	Send(data []byte) error                  //send data
	SendBuffer(buffer buffers.IBuffer) error //send buffer
	Close() error                            //close connection
	RemoteAddr() string                      //remote address
	Context() context.Context                //get context
	SetContext(ctx context.Context)          //set context
}

// IHandler handles connection lifecycle and messages.
type IHandler interface {
	OnConnect(c IConn)
	OnMessage(c IConn, data buffers.IBuffer)
	OnClose(c IConn, err error)
}

// IServer represents a server.
type IServer interface {
	Start() error
	Stop() error
	Addr() string
	Stats() StatsSnapshot
	GetConnManager() IConnManager
}

// IConnManager manages server connections.
type IConnManager interface {
	GetAllConns() map[int64]IConn //获取所有连接
	GetConn(id int64) IConn       //获取指定连接
	KickConn(id int64)            //踢出指定连接
}

// IClient represents a client.
type IClient interface {
	Connect() error
	Close() error
	Addr() string
	Stats() StatsSnapshot
}
