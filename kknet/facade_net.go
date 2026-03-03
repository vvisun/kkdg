package kknet

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// IConn represents a network connection.
type IConn interface {
	ID() CONN_ID        //unique connection id
	Close() error       //close connection
	RemoteAddr() string //remote address

	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	SendMsg(msg any) error

	BindUser(uid USER_ID)
	UnbindUser()
	GetUserId() USER_ID
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
	RangeAllConns(fn func(id CONN_ID, conn IConn) bool) //遍历所有连接, fn返回false时停止遍历
	GetConn(id CONN_ID) IConn                           //获取指定连接
	KickConn(id CONN_ID)                                //踢出指定连接
	GetCount() int                                      //获取连接数量
	KickUser(uid USER_ID)                               //踢出指定用户
	GetConnByUser(uid USER_ID) IConn                    //获取指定用户连接
	BindUser(c IConn, uid USER_ID)                      //绑定用户到连接
	UnbindUser(c IConn)                                 //解绑用户从连接
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
	IsConnected() bool

	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	SendMsg(msg any) error
}

// 通用网络事件处理器。总结自gnet和gws。
type INetHandler interface {
	// OnBoot 引擎(server/client)启动事件
	// OnBoot 在引擎准备好接受连接时触发。
	// 参数engine包含引擎信息和各种实用工具。
	// OnBoot fires when the engine is ready for accepting connections.
	// The parameter engine has information and various utilities.
	// @param engine 引擎
	// @return action 动作
	OnBoot(engine any) (action any)

	// OnShutdown 引擎(server/client)关闭事件
	// OnShutdown 在引擎关闭时触发。
	// 所有事件循环和连接关闭后调用。
	// OnShutdown fires when the engine is being shut down, it is called right after
	// all event-loops and connections are closed.
	// @param engine 引擎
	OnShutdown(engine any)

	// OnOpen 建立连接事件
	// WebSocket connection was successfully established
	// @param socket 连接
	OnOpen(socket any)

	// OnClose 关闭事件
	// 接收到了网络连接另一端发送的关闭帧, 或者IO过程中出现错误主动断开连接
	// 如果是前者, err可以断言为*CloseError
	// Received a close frame from the other end of the network connection, or disconnected voluntarily due to an error in the IO process
	// In the former case, err can be asserted as *CloseError
	// @param socket 连接
	// @param err 错误
	OnClose(socket any, err error)

	// OnPing 心跳探测事件
	// Received a ping frame
	// @param socket 连接
	// @param payload 心跳数据
	OnPing(socket any, payload []byte)

	// OnPong 心跳响应事件
	// Received a pong frame
	// @param socket 连接
	// @param payload 心跳数据
	OnPong(socket any, payload []byte)

	// OnMessage 消息事件
	// 如果开启了ParallelEnabled, 会并行地调用OnMessage; 没有做recover处理.
	// If ParallelEnabled is enabled, OnMessage is called in parallel. No recover is done.
	// @param socket 连接
	// @param message 消息
	OnMessage(socket any, message any)
}
