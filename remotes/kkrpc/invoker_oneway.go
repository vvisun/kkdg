package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/kklog"
)

type OneWayInvoker[T any] struct {
	sender ISender
	connId kknet.CONN_ID
	method string // 构造时验证并缓存，调用时不再 CheckOneWay
}

// NewOneWayInvoker 创建单向调用器。method 必须在 RegisterOneWayMethod 中已注册，否则 panic。
// 服务器端调用时 connId 为连接ID；客户端调用时 connId 为 会被忽略，直接发送给client所连接的server。
func NewOneWayInvoker[T any](sender ISender, connId kknet.CONN_ID) (OneWayInvoker[T], error) {
	method, ok := verifyOneWayMethod[T]()
	if !ok {
		kklog.Errorf("OneWayInvoker: method %q not registered for type %T", method, (*T)(nil))
		return OneWayInvoker[T]{}, kkerrors.ErrRpcMethodNotRegistered
	}
	return OneWayInvoker[T]{
		sender: sender,
		connId: connId,
		method: method,
	}, nil
}

// InvokeNR 无响应调用（单向调用）
// 服务器端调用时 connId 为连接ID；客户端调用时 connId 为 会被忽略，直接发送给client所连接的server。
func (i OneWayInvoker[T]) InvokeNR(ctx context.Context, req *T, opts CallConfig) error {
	if i.sender.getPending().IsClosed() {
		return kkerrors.ErrRpcConnClosed
	}
	bb, err := EncodeRpcFrame(FrameTypeOneway, 0, i.method, req, ctxDeadlineUnixMs(ctx))
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	err = i.sender.SendBuffer(i.connId, bb)
	if err != nil {
		return err
	}
	return nil
}

//----------------------------------------------------------------

// InvokeOneWay 无响应调用（单向调用）
// 服务器端调用时 connId 为连接ID；客户端调用时 connId 为 会被忽略，直接发送给client所连接的server。
func InvokeOneWay(ctx context.Context, sender ISender, connId kknet.CONN_ID, req any, opts CallConfig) error {
	method := gRpcManager.getMethod(req)
	if method == "" {
		return kkerrors.ErrRpcMethodNotRegistered
	}
	if sender.getPending().IsClosed() {
		return kkerrors.ErrRpcConnClosed
	}
	bb, err := EncodeRpcFrame(FrameTypeOneway, 0, method, req, ctxDeadlineUnixMs(ctx))
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	err = sender.SendBuffer(connId, bb)
	if err != nil {
		return err
	}
	return nil
}
