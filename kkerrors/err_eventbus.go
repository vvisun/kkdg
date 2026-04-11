package kkerrors

import "errors"

var (
	// 事件总线实例未设置
	ErrMissingEventbusInstance = errors.New("missing eventbus instance")
	// 事件处理器无效
	ErrInvalidHandler = errors.New("invalid handler")
)
