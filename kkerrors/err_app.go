package kkerrors

import "errors"

var (
	// 应用未初始化
	ErrAppNotInitialized = errors.New("app is not initialized")
	// 应用已初始化
	ErrAppInitialized = errors.New("app is already initialized")
	// 应用未关闭
	ErrAppNotShutdown = errors.New("app is not shutdown")
	// 应用已关闭
	ErrAppShutdown = errors.New("app is already shutdown")
)
