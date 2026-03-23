package kknet

import (
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// IConn represents a network connection.
type IConn interface {
	//unique connection id
	ID() CONN_ID
	//close connection
	Close() error
	//remote address
	RemoteAddr() string
	//发送二进制数据。内部会自动释放buffer
	SendBuffer(buffer *kkbuffer.ByteBuffer) error
	//发送结构体对象。内部会使用kkpacket编码。
	//前提：必须已正确配置 WriteOptions.MsgPacket（或通过 WithMsgPacket 设置）。
	//这是配置约束；未配置时属于必现错误，当前实现允许直接 panic。
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
	GetConn(id CONN_ID) IConn                           //获取指定连接
	KickConn(id CONN_ID)                                //踢出指定连接。关闭连接并从管理器中移除
	GetCount() int                                      //获取连接数量
	RangeAllConns(fn func(id CONN_ID, conn IConn) bool) //遍历所有连接, fn返回false时停止遍历
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
	//发送结构体对象。内部会使用kkpacket编码。
	//前提：必须已正确配置 WriteOptions.MsgPacket（或通过 WithMsgPacket 设置）。
	//这是配置约束；未配置时属于必现错误，当前实现允许直接 panic。
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
	//发送结构体对象。内部会使用kkpacket编码。
	//前提：必须已正确配置 WriteOptions.MsgPacket（或通过 WithMsgPacket 设置）。
	//这是配置约束；未配置时属于必现错误，当前实现允许直接 panic。
	SendMsg(msg any) error
}
