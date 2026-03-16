package kkrpc

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// RegisterReqRspMethod 注册请求响应方法。method的参数类型和返回类型必须为REQ和RSP。
// 相当于函数签名: func method(REQ) RSP
func RegisterReqRspMethod[REQ any, RSP any](methodMgr *MethodManager) error {
	if err := newReqResp[REQ, RSP](methodMgr, ""); err != nil {
		// 错误直接蹦就行，避免将影响延迟到运行期带来不可预知的错误
		kklog.Errorf("register reqrsp method error: %v", err)
		return err
	}
	return nil
}

// RegisterOneWayMethod 注册单向方法。method的参数类型必须为REQ。
// 相当于函数签名: func method(REQ)
func RegisterOneWayMethod[REQ any](methodMgr *MethodManager) error {
	if err := newOneWay[REQ](methodMgr, ""); err != nil {
		// 错误直接蹦就行，避免将影响延迟到运行期带来不可预知的错误
		kklog.Errorf("register oneway method error: %v", err)
		return err
	}
	return nil
}

// RegistReqRspHandler 注册请求响应方法 handler
func RegistReqRspHandler[T any, R any](receiver *RpcReceiver, call ReqRspHandlerFunc[T, R]) error {
	var pReq *T
	var pRsp *R
	fullName, selfDefineName := receiver.methodMgr.getMethodReqRsp(pReq, pRsp)
	if fullName == "" {
		kklog.Errorf("invalid req resp type %s %s", getObjectName(pReq), getObjectName(pRsp))
		return kkerrors.ErrRpcInvalidReqResp
	}
	h := &ReqRspHandler[T, R]{
		call:         call,
		method:       fullName,
		payloadCodec: receiver.methodMgr.payloadCodec,
	}
	receiver.reqrspMap[fullName] = h
	if selfDefineName != "" {
		receiver.reqrspMap[selfDefineName] = h
	}
	return nil
}

// RegistOneWayHandler 注册单向消息方法 handler
func RegistOneWayHandler[T any](receiver *RpcReceiver, call OneWayHandlerFunc[T]) error {
	var pReq *T
	fullName, selfDefineName := receiver.methodMgr.getMethodOneway(pReq)
	if fullName == "" {
		kklog.Errorf("invalid oneway type %s", getObjectName(pReq))
		return kkerrors.ErrRpcInvalidOneway
	}
	h := &OneWayHandler[T]{
		call:         call,
		method:       fullName,
		payloadCodec: receiver.methodMgr.payloadCodec,
	}
	receiver.onewayMap[fullName] = h
	if selfDefineName != "" {
		receiver.onewayMap[selfDefineName] = h
	}
	return nil
}
