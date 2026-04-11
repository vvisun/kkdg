package kkerrors

import "errors"

// -------------- for xnet -------------------
var (
	// 未找到IP地址
	ErrXNetNotFoundIPAddress = errors.New("xnet not found ip address")
)

// -------------- for xreflect -------------------
var (
	// 函数为nil
	ErrXreflectFuncIsNil = errors.New("xreflect func is nil")
	// 函数类型错误
	ErrXreflectFuncTypeError = errors.New("xreflect func type error")
)

// -------------- for kkcodec -------------------
var (
	// 无法解码为 proto.Message 类型
	ErrCodecCannotUnmarshalToProtoMessage = errors.New("codec cannot unmarshal to a value that not implements proto.Buffer")
	// 无法编码为 FlatBuffer 类型（需实现 FlatBufferPackable，即 *XxxT）
	ErrCodecCannotMarshalFlatBuffer = errors.New("codec cannot marshal to flatbuffer: value must implement FlatBufferPackable (*XxxT)")
	// 无法解码为 FlatBuffer 类型（需实现 FlatBufferTable，即 *Xxx）
	ErrCodecCannotUnmarshalFlatBuffer = errors.New("codec cannot unmarshal to flatbuffer: value must implement FlatBufferTable (*Xxx)")
)

// -------------- for tls -------------------
var (
	// 无效的证书文件
	ErrInvalidCertFile = errors.New("invalid cert file")
)
