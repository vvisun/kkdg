package kkerrors

import "errors"

var (
	// ErrClusterMemberNotFound 成员未找到
	ErrClusterMemberNotFound = errors.New("cluster member not found")
	// ErrClusterInvalidPacket 无效的消息包
	ErrClusterInvalidPacket = errors.New("cluster invalid packet")
	// ErrClusterNoMemberOfType 没有该类型的成员
	ErrClusterNoMemberOfType = errors.New("cluster no member of type")
	// ErrClusterInvalidCodec 无效的编解码器
	ErrClusterInvalidCodec = errors.New("cluster invalid codec")
	// ErrClusterNotConnected 不在连接状态
	ErrClusterNotConnected = errors.New("cluster not connected")
)
