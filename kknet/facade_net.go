package kknet

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// IConn represents a network connection.
type IConn interface {
	ID() CONN_ID        //unique connection id
	Close() error       //close connection
	RemoteAddr() string //remote address

	SetExtraData(userData any) //设置自定义数据
	GetExtraData() any         //获取自定义数据

	//发送二进制数据。内部会自动释放buffer
	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	//发送结构体对象。内部会使用kkpacket编码
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
	RangeAllConns(fn func(id CONN_ID, conn IConn) bool) //遍历所有连接, fn返回false时停止遍历
	GetConn(id CONN_ID) IConn                           //获取指定连接
	KickConn(id CONN_ID)                                //踢出指定连接
	GetCount() int                                      //获取连接数量
}

// IServer represents a server.
type IServer interface {
	Start() error
	Stop() error
	Addr() string
	Stats() StatsSnapshot
	GetConnManager() IConnManager

	//发送二进制数据。内部会自动释放buffer
	SendBuffer(connId CONN_ID, buffer *kkbuffer.ByteBuffer) error
	//发送结构体对象。内部会使用kkpacket编码
	SendMsg(connId CONN_ID, msg any) error
}

// IClient represents a client.
type IClient interface {
	Connect() error //like Start()
	Close() error   //like Stop()
	Addr() string
	Stats() StatsSnapshot //获取统计信息快照
	IsConnected() bool    //是否处于连接状态
	IsStopped() bool      //是否已停止

	//发送二进制数据。内部会自动释放buffer
	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	//发送结构体对象。内部会使用kkpacket编码
	SendMsg(msg any) error
}
