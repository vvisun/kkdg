package kkcluster

import "errors"

var (
	// ErrMemberNotFound 成员未找到
	ErrMemberNotFound = errors.New("member not found")
	// ErrInvalidPacket 无效的消息包
	ErrInvalidPacket = errors.New("invalid packet")
	// ErrNoMemberOfType 没有该类型的成员
	ErrNoMemberOfType = errors.New("no member of type")
)

type ClusterErrorCode int32

const (
	// 成功
	ClusterErrorCodeSuccess ClusterErrorCode = 0 + iota
	// 失败
	ClusterErrorCodeFail
	// 无效的请求
	ClusterErrorCodeInvalidRequest
	// 无效的响应
	ClusterErrorCodeInvalidResponse
	// 无效的数据
	ClusterErrorCodeInvalidData
	// 成员未找到
	ClusterErrorCodeMemberNotFound
	// 序列化失败
	ClusterErrorCodeMarshalFailed
	// 订阅失败
	ClusterErrorCodeSubscribeFailed
	// 发布失败
	ClusterErrorCodePublishFailed
	// 超时
	ClusterErrorCodeTimeout
)
