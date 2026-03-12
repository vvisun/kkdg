package kkrpc

import (
	"reflect"

	"github.com/vvisun/kkdg/utils/kklog"
)

// RegisterReqRspMethod 注册请求响应方法。method的参数类型和返回类型必须为REQ和RSP。
// 相当于函数签名: func method(REQ) RSP
func RegisterReqRspMethod[REQ any, RSP any](method string) {
	_, ok := newReqResp[REQ, RSP](method)
	if !ok {
		// 错误直接蹦就行，避免将影响延迟到运行期带来不可预知的错误
		kklog.PanicLog("register reqrsp method %s failed", method)
	}
}

// RegisterOneWayMethod 注册单向方法。method的参数类型必须为REQ。
// 相当于函数签名: func method(REQ)
func RegisterOneWayMethod[REQ any](method string) {
	_, ok := newOneWay[REQ](method)
	if !ok {
		// 错误直接蹦就行，避免将影响延迟到运行期带来不可预知的错误
		kklog.PanicLog("register oneway method %s failed", method)
	}
}

// RegistReqRspHandler 注册请求响应方法 handler
func RegistReqRspHandler[T any, R any](router *RpcReceiver, method string, call ReqRspHandlerFunc[T, R]) {
	h := &ReqRspHandler[T, R]{
		call:         call,
		method:       method,
		payloadCodec: router.payloadCodec,
	}
	router.hdMap[method] = h
}

// RegistOneWayHandler 注册单向消息方法 handler
func RegistOneWayHandler[T any](router *RpcReceiver, method string, call OneWayHandlerFunc[T]) {
	h := &OneWayHandler[T]{
		call:         call,
		method:       method,
		payloadCodec: router.payloadCodec,
	}
	router.oneWayMap[method] = h
}

//----------------------------------------------------------------

type ReqResp[REQ any, RSP any] struct {
	method string
}

type OneWay[REQ any] struct {
	method string
}

func newReqResp[REQ any, RSP any](method string) (*ReqResp[REQ, RSP], bool) {
	if method == "" {
		kklog.Errorf("method is empty")
		return nil, false
	}

	typeReq := reflect.TypeFor[*REQ]()
	typeRsp := reflect.TypeFor[*RSP]()

	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
	// if gRpcManager.peers[method] != nil {
	// 	kklog.Errorf("method %s already registered", method)
	// 	return nil, false
	// }
	if gRpcManager.type2methodReqRsp[typeReq] != "" {
		kklog.Errorf("type %s already registered", typeReq)
		return nil, false
	}
	if gRpcManager.type2methodReqRsp[typeRsp] != "" {
		kklog.Errorf("type %s already registered", typeRsp)
		return nil, false
	}

	p := &ReqResp[REQ, RSP]{
		method: method,
	}
	//gRpcManager.peers[method] = p
	gRpcManager.type2methodReqRsp[typeReq] = method
	gRpcManager.type2methodReqRsp[typeRsp] = method
	gRpcManager.method2typeReqRsp[method] = methodReqRsp{reqType: typeReq, rspType: typeRsp}

	typeReqValue := reflect.TypeFor[REQ]()
	typeRspValue := reflect.TypeFor[RSP]()
	gRpcManager.type2methodReqRsp[typeReqValue] = method
	gRpcManager.type2methodReqRsp[typeRspValue] = method

	return p, true
}

func newOneWay[REQ any](method string) (*OneWay[REQ], bool) {
	if method == "" {
		kklog.Errorf("method is empty")
		return nil, false
	}

	typeReq := reflect.TypeFor[*REQ]()

	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
	// if gRpcManager.oneWays[method] != nil {
	// 	kklog.Errorf("method %s already registered", method)
	// 	return nil, false
	// }
	if gRpcManager.type2methodOneWay[typeReq] != "" {
		kklog.Errorf("type %s already registered", typeReq)
		return nil, false
	}

	o := &OneWay[REQ]{
		method: method,
	}
	//gRpcManager.oneWays[method] = o
	gRpcManager.type2methodOneWay[typeReq] = method
	gRpcManager.method2typeOneWay[method] = methonOneWay{reqType: typeReq}

	typeReqValue := reflect.TypeFor[REQ]()
	gRpcManager.type2methodOneWay[typeReqValue] = method

	return o, true
}

// verifyReqRespMethod verifies at init that REQ/RSP types are registered for method. No runtime reflect on hot path.
func verifyReqRespMethod[REQ any, RSP any]() (string, bool) {
	typeReq := reflect.TypeFor[*REQ]()
	typeRsp := reflect.TypeFor[*RSP]()
	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
	mReq, ok1 := gRpcManager.type2methodReqRsp[typeReq]
	mRsp, ok2 := gRpcManager.type2methodReqRsp[typeRsp]
	if ok1 && ok2 && mReq == mRsp {
		return mReq, true
	}
	return "", false
}

// verifyOneWayMethod verifies at init that REQ type is registered for method. No runtime reflect on hot path.
func verifyOneWayMethod[REQ any]() (string, bool) {
	typeReq := reflect.TypeFor[*REQ]()
	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
	m, ok := gRpcManager.type2methodOneWay[typeReq]
	return m, ok
}
