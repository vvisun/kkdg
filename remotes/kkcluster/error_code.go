package kkcluster

import "fmt"

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

// ErrFromCode 将 ClusterErrorCode 转为 error，code 为 0 时返回 nil
func ErrFromCode(code ClusterErrorCode) error {
	if code == ClusterErrorCodeSuccess {
		return nil
	}
	return fmt.Errorf("cluster error: code=%d", code)
}
