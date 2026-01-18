package kkerrors

import "errors"

// -------------- for utils -------------------
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
