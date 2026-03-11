package kkerrors

import "errors"

// -------------- for kkpacket -------------------
var (
	// 无效的消息头类型
	ErrPktInvalidMsgHeadType = errors.New("kkpacket invalid message head type")
	// 无效的编解码器
	ErrPktInvalidCodec = errors.New("kkpacket invalid codec")
	// 解码失败
	ErrPktDecodeFailed = errors.New("kkpacket decode failed")
	// 数据太短，无法解码
	ErrPktDataTooShortToDecode = errors.New("kkpacket data too short to decode")
	// 编码失败
	ErrPktEncodeFailed = errors.New("kkpacket encode failed")
	// 无效的消息ID
	ErrPktInvalidMsgID = errors.New("kkpacket invalid message id")
	// 未注册该消息ID
	ErrPktMsgIDNotRegistered = errors.New("kkpacket message id not registered")
	// 未注册该消息类型
	ErrPktMsgTypeNotRegistered = errors.New("kkpacket message type not registered")
	// 未注册该消息的Handler
	ErrPktMsgHandlerNotRegistered = errors.New("kkpacket message handler not registered")
	// 数据太短，无法编码
	ErrPktDataTooShortToMarshal = errors.New("kkpacket data too short to marshal")
	// 数据太短，无法解码
	ErrPktDataTooShortToUnmarshal = errors.New("kkpacket data too short to unmarshal")
	// 值列表太短，无法编码
	ErrPktValueListTooShortToMarshal = errors.New("kkpacket value list too short to marshal")
	// 值列表太短，无法解码
	ErrPktValueListTooShortToUnmarshal = errors.New("kkpacket value list too short to unmarshal")
	// 值超出范围
	ErrPktValueOutOfRange = errors.New("kkpacket value out of range")
	// 消息ID已注册
	ErrPktMsgIDAlreadyRegistered = errors.New("kkpacket message id already registered")
	// 消息头部分名称未找到
	ErrPktHeadPartNameNotFound = errors.New("kkpacket head part name not found")
	// 名字长度和part长度不一致
	ErrPktNamesAndPartsLengthNotMatch = errors.New("kkpacket head names and parts length not match")
	// 消息头名字重复
	ErrPktHeadPartNameRepeated = errors.New("kkpacket head part name repeated")
	// 最大消息大小超出限制
	ErrPktMaxMessageSize = errors.New("kkpacket message size exceeds maximum")
	// 无效消息
	ErrPktInvalidMessage = errors.New("kkpacket invalid message")
	// 无效的长度字段字节数
	ErrPktInvalidLengthFieldByteCount = errors.New("kkpacket invalid length field byte count")
)
