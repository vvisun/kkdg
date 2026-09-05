package kkerrors

import "errors"

var (
	// 无效的节点ID
	ErrAppInvalidNodeID = errors.New("app invalid node id")
	// 无效的节点类型
	ErrAppInvalidNodeType = errors.New("app invalid node type")
	// 应用未初始化
	ErrAppNotInitialized = errors.New("app not initialized")
	// 应用已初始化
	ErrAppInitialized = errors.New("app already initialized")
	// 应用未关闭
	ErrAppNotShutdown = errors.New("app not shutdown")
	// 应用已关闭
	ErrAppShutdown = errors.New("app already shutdown")
	// 应用已启动
	ErrAppAlreadyStarted = errors.New("app already started")
	// 应用未启动
	ErrAppNotStarted = errors.New("app not started")
	// 应用只能在空状态添加组件
	ErrAppAddCompMustInNoneState = errors.New("app add comp must in none state")
	// 应用spawn actor失败
	ErrAppSpawnActorFailed = errors.New("app spawn actor failed")
)

// -------------- for transportor -------------------
var (
	// 应用传输层已停止
	ErrAppTransportorStopped = errors.New("app trans transportor stopped")
	// 应用传输层空sessionID
	ErrAppEmptySessionID = errors.New("app trans empty sessionID")
	// 应用传输层session未找到
	ErrAppSessionNotFound = errors.New("app trans session not found")
	// 应用传输层空消息字节
	ErrAppEmptyMsgBytes = errors.New("app trans empty msgBytes")
	// 应用传输层集群未初始化
	ErrAppClusterNotInitialized = errors.New("app trans cluster not initialized")
	// 应用传输层逻辑节点未注册
	ErrAppLogicNodeNotRegistered = errors.New("app trans logic node not registered")
	// 应用传输层逻辑分片未连接
	ErrAppLogicShardNotConnected = errors.New("app trans logic shard not connected")
	// decodeWorkers 数量与 SessionManager.workersCount 不一致
	ErrAppThreadWorkerMismatch = errors.New("app trans decode worker count mismatch")
	// OnSession 的 threadIdx 越界
	ErrAppInvalidThreadIdx = errors.New("app trans invalid threadIdx")
)
