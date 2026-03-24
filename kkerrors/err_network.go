package kkerrors

import "errors"

// -------------- for network -------------------
var (
	// 连接已关闭
	ErrNetConnectionClosed = errors.New("kknet connection is closed")
	// 接收队列已满（严格模式下）
	ErrNetRecvQueueFull = errors.New("kknet recv queue is full")
	// 异步发送队列已满（严格模式下）
	ErrNetSendQueueFull = errors.New("kknet send queue is full")
	// 服务器未启动
	ErrNetServerNotStarted = errors.New("kknet server is not started")
	// 客户端未连接
	ErrNetClientNotConnected = errors.New("kknet client is not connected")
	// 服务器已停止
	ErrNetServerStopped = errors.New("kknet server is stopped")
	// 重连尝试次数超出。重连失败。
	ErrNetReconnectAttemptsExceeded = errors.New("kknet reconnect attempts exceeded")
	// 连接未找到
	ErrNetConnNotFound = errors.New("kknet conn not found")
)
