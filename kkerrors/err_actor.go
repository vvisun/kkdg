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
