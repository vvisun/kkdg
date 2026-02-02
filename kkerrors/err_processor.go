package kkerrors

import "errors"

var (
	// 连接不在当前处理器中
	ErrConnNotInThisProcessor = errors.New("conn not in this processor")
)
