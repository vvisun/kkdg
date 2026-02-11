package kkerrors

import "errors"

// -------------- for network -------------------
var (
	// 连接已关闭
	ErrConnectionClosed = errors.New("connection is closed")
	// 异步发送队列已满（严格模式下）
	ErrSendQueueFull = errors.New("send queue is full")
	// 服务器未启动
	ErrServerNotStarted = errors.New("server is not started")
	// 客户端未连接
	ErrClientNotConnected = errors.New("client is not connected")
	// 无效消息
	ErrInvalidMessage = errors.New("invalid message")
	// 编解码错误
	ErrCodecError = errors.New("codec error")
	// 最大消息大小超出限制
	ErrMaxMessageSize = errors.New("message size exceeds maximum")
	// 服务器已停止
	ErrServerStopped = errors.New("server is stopped")
	// 无效的长度字段字节数
	ErrInvalidLengthFieldByteCount = errors.New("invalid length field byte count")
	// 连接未设置
	ErrConnNotSet = errors.New("connection is not set")
	// 重连尝试次数超出。重连失败。
	ErrReconnectAttemptsExceeded = errors.New("reconnect attempts exceeded")
	// 连接未找到
	ErrConnNotFound = errors.New("conn not found")
)
