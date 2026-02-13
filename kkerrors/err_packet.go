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
	// 未注册该消息的Handler
	ErrMsgHandlerNotRegistered = errors.New("message handler not registered")
	// 数据太短，无法编码
	ErrDataTooShortToMarshal = errors.New("data too short to marshal")
	// 数据太短，无法解码
	ErrDataTooShortToUnmarshal = errors.New("data too short to unmarshal")
	// 值列表太短，无法编码
	ErrValueListTooShortToMarshal = errors.New("value list too short to marshal")
	// 值列表太短，无法解码
	ErrValueListTooShortToUnmarshal = errors.New("value list too short to unmarshal")
)

// -------------- for kkcodec -------------------
var (
	// 无法解码为 proto.Message 类型
	ErrCannotUnmarshalToProtoMessage = errors.New("cannot unmarshal to a value that not implements proto.Buffer")
	// 无法编码为 FlatBuffer 类型（需实现 FlatBufferPackable，即 *XxxT）
	ErrCannotMarshalFlatBuffer = errors.New("cannot marshal to flatbuffer: value must implement FlatBufferPackable (*XxxT)")
	// 无法解码为 FlatBuffer 类型（需实现 FlatBufferTable，即 *Xxx）
	ErrCannotUnmarshalFlatBuffer = errors.New("cannot unmarshal to flatbuffer: value must implement FlatBufferTable (*Xxx)")
)
