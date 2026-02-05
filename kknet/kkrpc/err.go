package kkrpc

import (
	"context"
	"errors"
)

var (
	// 客户端未连接
	ErrClientNotConnected = errors.New("gnrpc: client not connected")
	// 客户端已关闭
	ErrClientClosed = errors.New("gnrpc: client closed")
	// 方法未找到
	ErrMethodNotFound = errors.New("gnrpc: method not found")
	// 无效的帧
	ErrInvalidFrame = errors.New("gnrpc: invalid frame")
	// 超时
	ErrDeadlineExceeded = context.DeadlineExceeded
	// 超时
	ErrTimeout = errors.New("timeout")
	// 连接未找到
	ErrConnNotFound = errors.New("conn not found")
	// 服务器未启动
	ErrServerNotStarted = errors.New("server not started")
)
