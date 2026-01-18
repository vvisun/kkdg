package kkerrors

import "errors"

var (
	// 未找到IP地址
	ErrNotFoundIPAddress = errors.New("not found ip address")
	// 意外EOF
	ErrUnexpectedEOF = errors.New("unexpected EOF")
	// 无效的whence
	ErrInvalidWhence = errors.New("invalid whence")
	// 负位置
	ErrNegativePosition = errors.New("negative position")
	// v不是[]byte类型
	ErrNotByteSlice = errors.New("v is not a []byte")
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

var (
	// 无效的消息头类型
	ErrInvalidMsgHeadType = errors.New("invalid message head type")
	// 无效的编解码器
	ErrInvalidCodec = errors.New("invalid codec")
	// 解码失败
	ErrDecodeFailed = errors.New("decode failed")
	// 编码失败
	ErrEncodeFailed = errors.New("encode failed")
	// 无效的消息ID
	ErrInvalidMsgID = errors.New("invalid message id")
	// 未注册该消息ID
	ErrMsgIDNotRegistered = errors.New("message id not registered")
	// 未注册该消息类型
	ErrMsgTypeNotRegistered = errors.New("message type not registered")
)

var (
	// 组件未初始化
	ErrComponentNotInitialized = errors.New("component is not initialized")
	// 组件已初始化
	ErrComponentInitialized = errors.New("component is already initialized")
	// 组件未关闭
	ErrComponentNotShutdown = errors.New("component is not shutdown")
	// 组件已关闭
	ErrComponentShutdown = errors.New("component is already shutdown")
)
