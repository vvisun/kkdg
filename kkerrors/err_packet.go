package kkerrors

import "errors"

// -------------- for kkpacket -------------------
var (
	// 无效的消息头类型
	ErrInvalidMsgHeadType = errors.New("invalid message head type")
	// 无效的编解码器
	ErrInvalidCodec = errors.New("invalid codec")
	// 解码失败
	ErrDecodeFailed = errors.New("decode failed")
	// 数据太短，无法解码
	ErrDataTooShortToDecode = errors.New("data too short to decode")
	// 编码失败
	ErrEncodeFailed = errors.New("encode failed")
	// 无效的消息ID
	ErrInvalidMsgID = errors.New("invalid message id")
	// 未注册该消息ID
	ErrMsgIDNotRegistered = errors.New("message id not registered")
	// 未注册该消息类型
	ErrMsgTypeNotRegistered = errors.New("message type not registered")
)
