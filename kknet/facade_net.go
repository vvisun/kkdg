package kknet

import (
	"context"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// CONN_ID is the type of connection ID.
type CONN_ID = int64

// USER_ID is the type of user ID.
type USER_ID = int64

// counter for connection ID. unique id for the connection.
var connIDCounter atomic.Int64

// NextConnID returns a unique connection ID.
func NextConnID() CONN_ID {
	return connIDCounter.Add(1)
}

// IConn represents a network connection.
type IConn interface {
	ID() CONN_ID                    //unique connection id
	Close() error                   //close connection
	RemoteAddr() string             //remote address
	Context() context.Context       //get context
	SetContext(ctx context.Context) //set context

	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	SendMsg(msg any) error
}

// IConnLifecycleHandler handles connection lifecycle.
type IConnLifecycleHandler interface {
	/* OnConnect is called when the connection is established.
	@param c IConn 连接
	*/
	OnConnect(c IConn)
	/* OnClose is called when the connection is closed.
	@param c IConn 连接
	@param err error 错误（关闭原因）。
	*/
	OnClose(c IConn, err error)
}

// IConnManager manages server connections.
type IConnManager interface {
	GetAllConns() map[int64]IConn //获取所有连接
	GetConn(id CONN_ID) IConn     //获取指定连接
	KickConn(id CONN_ID)          //踢出指定连接
	GetCount() int64              //获取连接数量
}

// IServer represents a server.
type IServer interface {
	Start() error
	Stop() error
	Addr() string
	Stats() StatsSnapshot
	GetConnManager() IConnManager

	SendBuffer(connId CONN_ID, buffer *kkbuffer.ByteBuffer) error
	SendMsg(connId CONN_ID, msg any) error
}

// IClient represents a client.
type IClient interface {
	Connect() error //like Start()
	Close() error   //like Stop()
	Addr() string
	Stats() StatsSnapshot
	SetContext(ctx context.Context)

	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	SendMsg(msg any) error
}
