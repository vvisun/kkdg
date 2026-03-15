package kkerrors

import (
	"context"
	"errors"
)

var (
	// Rpc已关闭
	ErrRpcClosed = errors.New("kkrpc: closed")
	// 请求队列已满
	ErrRpcQueueFull = errors.New("kkrpc: queue full")
	// 客户端未连接
	ErrRpcNotConnected = errors.New("kkrpc: not connected")
	// 客户端已关闭
	ErrRpcConnClosed = errors.New("kkrpc: conn closed")
	// 方法未找到
	ErrRpcMethodNotFound = errors.New("kkrpc: method not found")
	// 无效的帧
	ErrRpcInvalidFrame = errors.New("kkrpc: invalid frame")
	// 超时
	ErrRpcDeadlineExceeded = context.DeadlineExceeded
	// 超时
	ErrRpcTimeout = errors.New("kkrpc: timeout")
	// 无效的请求ID
	ErrRpcInvalidRequestID = errors.New("kkrpc: invalid request id")
	// 无效的帧类型
	ErrRpcInvalidFrameType = errors.New("kkrpc: invalid frame type")

	// 无效的请求响应类型
	ErrRpcInvalidReqResp = errors.New("kkrpc: invalid req resp type")
	// 无效的单向类型
	ErrRpcInvalidOneway = errors.New("kkrpc: invalid oneway type")
	// 方法未注册
	ErrRpcMethodNotRegistered = errors.New("kkrpc: method not registered")
	// 重复注册
	ErrRpcMethodAlreadyRegistered = errors.New("kkrpc: method already registered")
)
