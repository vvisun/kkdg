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
	// 应用已启动
	ErrAppAlreadyStarted = errors.New("app is already started")
	// 应用未启动
	ErrAppNotStarted = errors.New("app is not started")
	// 应用只能在空状态添加组件
	ErrAppAddCompMustInNoneState = errors.New("app can only add component in none state")
	// 应用spawn actor失败
	ErrAppSpawnActorFailed = errors.New("app spawn actor failed")
)

// -------------- for session -------------------
var (
	ErrEmptySessionID         = errors.New("empty sessionID")
	ErrSessionNotFound        = errors.New("session not found")
	ErrEmptyMsgBytes          = errors.New("empty msgBytes")
	ErrClusterNotInitialized  = errors.New("cluster not initialized")
	ErrLogicNodeNotRegistered = errors.New("logic node not registered")
	ErrLogicShardNotConnected = errors.New("logic shard not connected")
	ErrTransportorStopped     = errors.New("transportor stopped")
)
