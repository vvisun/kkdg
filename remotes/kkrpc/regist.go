package kkrpc

import (
	"reflect"

	"github.com/vvisun/kkdg/utils/kklog"
)

// RegisterReqRspMethod 注册请求响应方法。method的参数类型和返回类型必须为REQ和RSP。
// 相当于函数签名: func method(REQ) RSP
func RegisterReqRspMethod[REQ any, RSP any](method string, methodMgr *MethodManager) {
	ok := newReqResp[REQ, RSP](method, methodMgr)
	if !ok {
		// 错误直接蹦就行，避免将影响延迟到运行期带来不可预知的错误
		kklog.PanicLog("register reqrsp method %s failed", method)
	}
}

// RegisterOneWayMethod 注册单向方法。method的参数类型必须为REQ。
// 相当于函数签名: func method(REQ)
func RegisterOneWayMethod[REQ any](method string, methodMgr *MethodManager) {
	ok := newOneWay[REQ](method, methodMgr)
	if !ok {
		// 错误直接蹦就行，避免将影响延迟到运行期带来不可预知的错误
		kklog.PanicLog("register oneway method %s failed", method)
	}
}

// RegistReqRspHandler 注册请求响应方法 handler
func RegistReqRspHandler[T any, R any](router *rpcReceiver, method string, call ReqRspHandlerFunc[T, R]) {
	h := &ReqRspHandler[T, R]{
		call:         call,
		method:       method,
		payloadCodec: router.methodMgr.payloadCodec,
	}
	router.hdMap[method] = h
}

// RegistOneWayHandler 注册单向消息方法 handler
func RegistOneWayHandler[T any](router *rpcReceiver, method string, call OneWayHandlerFunc[T]) {
	h := &OneWayHandler[T]{
		call:         call,
		method:       method,
		payloadCodec: router.methodMgr.payloadCodec,
	}
	router.oneWayMap[method] = h
}

//----------------------------------------------------------------

func newReqResp[REQ any, RSP any](method string, methodMgr *MethodManager) bool {
	if method == "" {
		kklog.Errorf("method is empty")
		return false
	}

	typeReq := reflect.TypeFor[*REQ]()
	typeRsp := reflect.TypeFor[*RSP]()

	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()

	if methodMgr.type2methodReqRsp[typeReq] != "" && methodMgr.type2methodReqRsp[typeReq] != method {
		kklog.Errorf("type %s already registered", typeReq)
		return false
	}
	if methodMgr.type2methodReqRsp[typeRsp] != "" && methodMgr.type2methodReqRsp[typeRsp] != method {
		kklog.Errorf("type %s already registered", typeRsp)
		return false
	}

	methodMgr.type2methodReqRsp[typeReq] = method
	methodMgr.type2methodReqRsp[typeRsp] = method
	methodMgr.method2typeReqRsp[method] = methodReqRsp{reqType: typeReq, rspType: typeRsp}

	typeReqValue := reflect.TypeFor[REQ]()
	typeRspValue := reflect.TypeFor[RSP]()
	methodMgr.type2methodReqRsp[typeReqValue] = method
	methodMgr.type2methodReqRsp[typeRspValue] = method

	return true
}

func newOneWay[REQ any](method string, methodMgr *MethodManager) bool {
	if method == "" {
		kklog.Errorf("method is empty")
		return false
	}

	typeReq := reflect.TypeFor[*REQ]()

	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()

	if methodMgr.type2methodOneWay[typeReq] != "" && methodMgr.type2methodOneWay[typeReq] != method {
		kklog.Errorf("type %s already registered", typeReq)
		return false
	}

	methodMgr.type2methodOneWay[typeReq] = method
	methodMgr.method2typeOneWay[method] = methodOneWay{reqType: typeReq}

	typeReqValue := reflect.TypeFor[REQ]()
	methodMgr.type2methodOneWay[typeReqValue] = method

	return true
}

// verifyReqRespMethod verifies at init that REQ/RSP types are registered for method. No runtime reflect on hot path.
func verifyReqRespMethod[REQ any, RSP any](methodMgr *MethodManager) (string, bool) {
	typeReq := reflect.TypeFor[*REQ]()
	typeRsp := reflect.TypeFor[*RSP]()
	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()
	mReq, ok1 := methodMgr.type2methodReqRsp[typeReq]
	mRsp, ok2 := methodMgr.type2methodReqRsp[typeRsp]
	if ok1 && ok2 && mReq == mRsp {
		return mReq, true
	}
	return "", false
}

// verifyOneWayMethod verifies at init that REQ type is registered for method. No runtime reflect on hot path.
func verifyOneWayMethod[REQ any](methodMgr *MethodManager) (string, bool) {
	typeReq := reflect.TypeFor[*REQ]()
	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()
	m, ok := methodMgr.type2methodOneWay[typeReq]
	return m, ok
}
