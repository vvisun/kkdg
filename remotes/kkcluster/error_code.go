package kkcluster

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
