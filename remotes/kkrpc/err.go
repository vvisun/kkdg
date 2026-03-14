package kkrpc

import (
	"errors"
	"fmt"

	"github.com/vvisun/kkdg/kkerrors"
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
	ErrorCodeConnClosed                            //连接已关闭（closeAll 时通知 pending callback）
)

func ErrRpc(code int32, msg string) error {
	if code == ErrorCodeSuccess {
		return nil
	}
	if code == ErrorCodeConnClosed {
		return kkerrors.ErrRpcConnClosed
	}
	if msg == "" {
		return fmt.Errorf("rpc error: code=%d", code)
	}
	return errors.New(msg)
}
