package kkrpc

import (
	"errors"
	"fmt"
)

type ErrorCode = int32

const (
	ErrorCodeSuccess          ErrorCode = 0 + iota //成功
	ErrorCodeFailed                                //未知错误
	ErrorCodeMethodNotFound                        //远程方法未找到
	ErrorCodeMethodRetErr                          //远程方法执行错误
	ErrorCodeTimeout                               //超时
	ErrorCodeInvalidRequest                        //无效的请求
	ErrorCodeInvalidResponse                       //无效的响应
	ErrorCodeInvalidData                           //无效的数据
	ErrorCodeInvalidFrame                          //无效的帧
	ErrorCodeInvalidFrameType                      //无效的帧类型
	ErrorCodeInvalidReqResp                        //无效的请求响应类型
)

func ErrRpc(code int32, msg string) error {
	if code == ErrorCodeSuccess {
		return nil
	}
	if msg == "" {
		return fmt.Errorf("rpc error: code=%d", code)
	}
	return errors.New(msg)
}
