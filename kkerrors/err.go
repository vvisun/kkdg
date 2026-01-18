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

var (
	// 无效包
	ErrInvalidPacket = errors.New("invalid packet")
	// 无效编解码器
	ErrInvalidCodec = errors.New("invalid codec")
	// 解码失败
	ErrDecodeFailed = errors.New("decode failed")
	// 无效消息ID
	ErrInvalidMsgID = errors.New("invalid message id")
	// 未注册该消息ID
	ErrMsgIDNotRegistered = errors.New("message id not registered")
	// 编码失败
	ErrEncodeFailed = errors.New("encode failed")
	// 未注册该消息类型
	ErrMsgTypeNotRegistered = errors.New("message type not registered")
	// 无效头类型
	ErrInvalidHeadType = errors.New("invalid head type")
)
