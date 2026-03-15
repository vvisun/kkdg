package kkrpc

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

// RegisterReqRspMethod 注册请求响应方法。method的参数类型和返回类型必须为REQ和RSP。
// 相当于函数签名: func method(REQ) RSP
func RegisterReqRspMethod[REQ any, RSP any](method string, methodMgr *MethodManager) error {
	if err := newReqResp[REQ, RSP](method, methodMgr); err != nil {
		// 错误直接蹦就行，避免将影响延迟到运行期带来不可预知的错误
		kklog.Errorf("register reqrsp method %s error: %v", method, err)
		return err
	}
	return nil
}

// RegisterOneWayMethod 注册单向方法。method的参数类型必须为REQ。
// 相当于函数签名: func method(REQ)
func RegisterOneWayMethod[REQ any](method string, methodMgr *MethodManager) error {
	if err := newOneWay[REQ](method, methodMgr); err != nil {
		// 错误直接蹦就行，避免将影响延迟到运行期带来不可预知的错误
		kklog.Errorf("register oneway method %s error: %v", method, err)
		return err
	}
	return nil
}

// RegistReqRspHandler 注册请求响应方法 handler
func RegistReqRspHandler[T any, R any](receiver *RpcReceiver, call ReqRspHandlerFunc[T, R]) error {
	var vT *T
	var vR *R
	method := receiver.methodMgr.getMethodReqRsp(vT, vR)
	if method == "" {
		kklog.Errorf("invalid req resp type %s %s", xreflect.ObjectTypeName(&vT), xreflect.ObjectTypeName(&vR))
		return kkerrors.ErrRpcInvalidReqResp
	}
	h := &ReqRspHandler[T, R]{
		call:         call,
		method:       method,
		payloadCodec: receiver.methodMgr.payloadCodec,
	}
	receiver.hdMap[method] = h
	return nil
}

// RegistOneWayHandler 注册单向消息方法 handler
func RegistOneWayHandler[T any](receiver *RpcReceiver, call OneWayHandlerFunc[T]) error {
	var vT T
	m := receiver.methodMgr.getMethodOneway(&vT)
	if m == "" {
		kklog.Errorf("invalid oneway type %s", m)
		return kkerrors.ErrRpcInvalidOneWay
	}
	method := m
	h := &OneWayHandler[T]{
		call:         call,
		method:       method,
		payloadCodec: receiver.methodMgr.payloadCodec,
	}
	receiver.oneWayMap[method] = h
	return nil
}
