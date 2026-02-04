package kkerrors

import "errors"

var (
	// 方法未找到
	ErrMethodNotFound = errors.New("method not found")
	// 无效的帧
	ErrInvalidFrame = errors.New("invalid frame")
	// 超时
	ErrTimeout = errors.New("timeout")
	// 连接未找到
	ErrConnNotFound = errors.New("conn not found")
)
