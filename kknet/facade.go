package kknet

import (
	"context"
	"sync/atomic"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers"
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

	// 异步发送数据。该方法会回收buffer，外部无需手动释放，也不可再使用该buffer。
	SendBuffer(buffer buffers.IBuffer) error
	// 同步发送数据。该方法会回收buffer，外部无需手动释放，也不可再使用该buffer。
	//SendBufferSync(buffer buffers.IBuffer) error
}

type (
	// IMsgHandler is a handler for messages.
	IMsgHandler interface {
		/*OnMsg is called when a message is received.
		@param connId CONN_ID 连接ID
		@param msg any 消息对象（object）
		@param msgID kkpacket.MSGID 消息ID
		@note 外部需记得释放消息对象！！！否则消息对象得不到回收，性能反而更低！！！
		@note 外部自行用解码器解码（内置的解码器见kkpacket/parser.go）
		*/
		OnMsg(connId CONN_ID, msg any, msgID kkpacket.MSGID)
	}

	// IRawHandler is a handler for raw data.
	IRawHandler interface {
		/*OnRaw is called when a raw data is received.
			@param connId CONN_ID 连接ID
			@param data buffers.IBuffer [length,message]
		    @note 外部需记得释放buffer！！！否则buffer得不到回收，性能反而更低！！！
		    @note 外部自行用解码器解码（内置的解码器见kkpacket/parser.go）
		*/
		OnRaw(connId CONN_ID, data buffers.IBuffer)
	}
)

type INewHandler interface {
	/* OnConnect is called when the connection is established.
	@param c IConn 连接
	*/
	OnConnect(c IConn)
	/* OnClose is called when the connection is closed.
	@param c IConn 连接
	@param err error 错误（关闭原因）。nil表示正常关闭，非nil表示异常关闭
	*/
	OnClose(c IConn, err error)
}

// todo: deprecated, use IMsgHandler and IRawHandler instead.
// IHandler handles connection lifecycle and messages.
type IHandler interface {
	// OnConnect is called when the connection is established.
	// @param c IConn 连接
	OnConnect(c IConn)
	// OnMessage is called when a message is received.
	// @param c IConn 连接
	// @param data buffers.IBuffer 消息数据(message body)。
	// 外部自行用解码器解码（内置的解码器见kkpacket/message_parser.go）
	// 注意：外部需记得释放buffer！！！否则buffer得不到回收，性能反而更低！！！
	OnMessage(c IConn, data buffers.IBuffer)
	// OnClose is called when the connection is closed.
	// @param c IConn 连接
	// @param err error 错误（关闭原因）。nil表示正常关闭，非nil表示异常关闭
	OnClose(c IConn, err error)
}

// IConnManager manages server connections.
type IConnManager interface {
	GetAllConns() map[int64]IConn //获取所有连接
	GetConn(id int64) IConn       //获取指定连接
	KickConn(id int64)            //踢出指定连接
}

// IServer represents a server.
type IServer interface {
	Start() error
	Stop() error
	Addr() string
	Stats() StatsSnapshot
	GetConnManager() IConnManager
}

// IClient represents a client.
type IClient interface {
	Connect() error //like Start()
	Close() error   //like Stop()
	Addr() string
	Stats() StatsSnapshot
	SetContext(ctx context.Context)
	//SendBuffer 异步发送数据。该方法会回收buffer，外部无需手动释放，也不可再使用该buffer。
	SendBuffer(buffer buffers.IBuffer) error
}
