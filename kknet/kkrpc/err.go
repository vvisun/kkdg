package kkrpc

import (
	"context"
	"errors"
)

var (
	// 客户端未连接
	ErrClientNotConnected = errors.New("kkrpc: client not connected")
	// 客户端已关闭
	ErrClientClosed = errors.New("kkrpc: client closed")
	// 方法未找到
	ErrMethodNotFound = errors.New("kkrpc: method not found")
	// 无效的帧
	ErrInvalidFrame = errors.New("kkrpc: invalid frame")
	// 超时
	ErrDeadlineExceeded = context.DeadlineExceeded
	// 超时
	ErrTimeout = errors.New("kkrpc: timeout")
	// 连接未找到
	ErrConnNotFound = errors.New("kkrpc: conn not found")
	// 服务器未启动
	ErrServerNotStarted = errors.New("kkrpc: server not started")
	// 无效的请求ID
	ErrInvalidRequestID = errors.New("kkrpc: invalid request id")
	// 无效的帧类型
	ErrInvalidFrameType = errors.New("kkrpc: invalid frame type")
	// 无效的请求响应类型
	ErrInvalidReqResp = errors.New("kkrpc: invalid req resp type")
)
