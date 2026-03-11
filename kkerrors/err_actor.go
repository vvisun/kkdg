package kkerrors

import "errors"

var (
	// actor 未找到
	ErrActorNotFound = errors.New("actor not found")
	// actor 无效的actorKey
	ErrActorInvalidActorKey = errors.New("invalid actor key")
	// actor 无效的actorKeys
	ErrActorInvalidActorKeys = errors.New("invalid actor keys")
	// actor 无效的nodeId
	ErrActorInvalidNodeId = errors.New("invalid node id")
	// actor 添加无效的PID
	ErrActorAddInvalidPID = errors.New("invalid pid")
	// actor 添加无效的node
	ErrActorAddInvalidNode = errors.New("invalid node")
)

var (
	// actor 远程传输未配置
	ErrActorRemoteTransportNotConfigured = errors.New("remote actor transport not configured")
	// actor 远程接收器未设置
	ErrActorRemoteReceiverNotSet = errors.New("remote actor receiver not set")
	// actor 异步请求回调为空
	ErrActorAsyncCallbackNil = errors.New("actor async callback is nil")
	// actor 远程目标无效
	ErrActorRemoteInvalidTarget = errors.New("invalid remote actor target")
	// actor 远程消息类型未注册
	ErrActorRemoteMsgTypeNotRegistered = errors.New("remote actor message type not registered")
)
