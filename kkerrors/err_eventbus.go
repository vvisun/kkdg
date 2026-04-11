package kkerrors

import "errors"

var (
	// 事件总线实例未设置
	ErrMissingEventbusInstance = errors.New("missing eventbus instance")
	// 事件总线未设置
	ErrEventbusNotSet = errors.New("eventbus not set")
	// 事件总线已设置
	ErrEventbusAlreadySet = errors.New("eventbus already set")
	// 事件总线未初始化
	ErrEventbusNotInitialized = errors.New("eventbus not initialized")
	// 事件总线已初始化
	ErrEventbusAlreadyInitialized = errors.New("eventbus already initialized")
	// 事件总线未关闭
	ErrEventbusNotClosed = errors.New("eventbus not closed")
	// 事件总线已关闭
	ErrEventbusAlreadyClosed = errors.New("eventbus already closed")
	// 事件处理器无效
	ErrInvalidHandler = errors.New("invalid handler")
)
