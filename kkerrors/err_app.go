package kkerrors

import "errors"

var (
	// 无效的节点ID
	ErrInvalidNodeID = errors.New("invalid node id")
	// 无效的节点类型
	ErrInvalidNodeType = errors.New("invalid node type")
	// 应用未初始化
	ErrAppNotInitialized = errors.New("app is not initialized")
	// 应用已初始化
	ErrAppInitialized = errors.New("app is already initialized")
	// 应用未关闭
	ErrAppNotShutdown = errors.New("app is not shutdown")
	// 应用已关闭
	ErrAppShutdown = errors.New("app is already shutdown")
)

// -------------- for session -------------------
var (
	ErrEmptySessionID         = errors.New("empty sessionID")
	ErrSessionNotFound        = errors.New("session not found")
	ErrEmptyMsgBytes          = errors.New("empty msgBytes")
	ErrClusterNotInitialized  = errors.New("cluster not initialized")
	ErrLogicNodeNotRegistered = errors.New("logic node not registered")
)
