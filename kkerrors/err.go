package kkerrors

import "errors"

var (
	ErrNotFoundIPAddress = errors.New("not found ip address")
	ErrUnexpectedEOF     = errors.New("unexpected EOF")
	ErrInvalidWhence     = errors.New("invalid whence")
	ErrNegativePosition  = errors.New("negative position")
	ErrNotByteSlice      = errors.New("v is not a []byte")
)

var (
	// 连接已关闭
	ErrConnectionClosed = errors.New("connection is closed")
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
)
