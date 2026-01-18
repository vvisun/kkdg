package kkerrors

import "errors"

// -------------- for component -------------------
var (
	// 组件未初始化
	ErrComponentNotInitialized = errors.New("component is not initialized")
	// 组件已初始化
	ErrComponentInitialized = errors.New("component is already initialized")
	// 组件未关闭
	ErrComponentNotShutdown = errors.New("component is not shutdown")
	// 组件已关闭
	ErrComponentShutdown = errors.New("component is already shutdown")
)
