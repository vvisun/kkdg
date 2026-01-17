package kknet

import (
	"context"
	"net"
)

// IMessageHandler 消息处理器接口
type IMessageHandler interface {
	// OnMessage 处理接收到的消息
	// conn: 连接对象
	// data: 消息数据
	OnMessage(conn IConnection, data []byte) error
}

// IConnectionHandler 连接处理器接口
type IConnectionHandler interface {
	// OnConnect 连接建立时调用
	OnConnect(conn IConnection)
	// OnDisconnect 连接断开时调用
	OnDisconnect(conn IConnection)
	// OnError 发生错误时调用
	OnError(conn IConnection, err error)
}

// ICodec 编解码器接口
type ICodec interface {
	// Encode 编码消息
	Encode(data []byte) ([]byte, error)
	// Decode 解码消息，返回解码后的数据和剩余数据
	Decode(data []byte) ([]byte, []byte, error)
}

// IConnection 连接接口
type IConnection interface {
	// ID 返回连接ID
	ID() string
	// RemoteAddr 返回远程地址
	RemoteAddr() net.Addr
	// LocalAddr 返回本地地址
	LocalAddr() net.Addr
	// Send 发送数据
	Send(data []byte) error
	// Close 关闭连接
	Close() error
	// Context 返回连接的上下文
	Context() context.Context
	// SetValue 设置连接上下文值
	SetValue(key, value interface{})
	// GetValue 获取连接上下文值
	GetValue(key interface{}) interface{}
}

// IServer 服务器接口
type IServer interface {
	// Start 启动服务器
	Start(addr string) error
	// Stop 停止服务器
	Stop() error
	// Broadcast 广播消息给所有连接
	Broadcast(data []byte) error
	// GetConnection 根据ID获取连接
	GetConnection(id string) (IConnection, bool)
	// GetConnections 获取所有连接
	GetConnections() []IConnection
}

// IClient 客户端接口
type IClient interface {
	// Connect 连接到服务器
	Connect(addr string) error
	// Disconnect 断开连接
	Disconnect() error
	// Send 发送数据
	Send(data []byte) error
	// IsConnected 是否已连接
	IsConnected() bool
}
