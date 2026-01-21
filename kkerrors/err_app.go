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
	// 重复添加
	ErrComponentAlreadyAdded = errors.New("component already added")
	// 重复移除
	ErrComponentAlreadyRemoved = errors.New("component already removed")
	// 重复设置父组件
	ErrComponentAlreadySetParent = errors.New("component already set parent")
	// 重复获取父组件
	ErrComponentAlreadyGetParent = errors.New("component already get parent")
	// 重复获取子组件
	ErrComponentAlreadyGetChildren = errors.New("component already get children")
)
