package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

type OneWayInvoker[T any] struct {
	sender ISender
	method string // 构造时验证并缓存，调用时不再 CheckOneWay
}

// NewOneWayInvoker 创建单向调用器。method 必须在 RegisterOneWayMethod 中已注册。
//
//	服务器端调用时 connId 为连接ID；客户端调用时 connId 为 会被忽略，直接发送给client所连接的server。
func NewOneWayInvoker[T any](sender ISender) (OneWayInvoker[T], error) {
	method, ok := verifyOneWayMethod[T](sender.getMethodMgr())
	if !ok {
		kklog.Errorf("OneWayInvoker: method %q not registered for type %T", method, (*T)(nil))
		return OneWayInvoker[T]{}, kkerrors.ErrRpcMethodNotRegistered
	}
	return OneWayInvoker[T]{
		sender: sender,
		method: method,
	}, nil
}

// InvokeNR 无响应调用（单向调用）
//
//	服务器端调用时 connId 为连接ID；客户端调用时 connId 为 会被忽略，直接发送给client所连接的server。
func (i OneWayInvoker[T]) InvokeNR(ctx context.Context, req *T, opts CallConfig) error {
	sender := i.sender
	pending := sender.getPending()
	if pending.IsClosed() {
		return kkerrors.ErrRpcConnClosed
	}
	if pending.stats != nil {
		pending.stats.AddOnewayStart()
	}
	bb, err := EncodeRpcFrame(sender.getMethodMgr(), FrameTypeOneway, 0, i.method, req, ctxDeadlineUnixMs(ctx))
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		if pending.stats != nil {
			pending.stats.AddOnewayError()
			pending.stats.AddInternalError()
		}
		return err
	}
	err = sender.SendBuffer(opts.ConnId, bb)
	if err != nil {
		if pending.stats != nil {
			pending.stats.AddOnewayError()
			pending.stats.AddInternalError()
		}
		return err
	}
	return nil
}
