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
