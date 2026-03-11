package kkrpc

import (
	"fmt"
	"reflect"

	"github.com/vvisun/kkdg/utils/kklog"
)

// RegisterReqRspMethod 注册请求响应方法。method的参数类型和返回类型必须为REQ和RSP。
// 相当于函数签名: func method(REQ) RSP
func RegisterReqRspMethod[REQ any, RSP any](method string) {
	_, ok := newReqResp[REQ, RSP](method)
	if !ok {
		panic(fmt.Sprintf("register reqrsp method %s failed", method))
	}
}

// RegisterOneWayMethod 注册单向方法。method的参数类型必须为REQ。
// 相当于函数签名: func method(REQ)
func RegisterOneWayMethod[REQ any](method string) {
	_, ok := newOneWay[REQ](method)
	if !ok {
		panic(fmt.Sprintf("register oneway method %s failed", method))
	}
}

// RegistReqRspHandler 注册请求响应方法 handler
func RegistReqRspHandler[T any, R any](router *RpcReceiver, method string, call ReqRspHandlerFunc[T, R]) {
	h := &ReqRspHandler[T, R]{
		call:   call,
		method: method,
	}
	router.hdMap[method] = h
}

// RegistOneWayHandler 注册单向消息方法 handler
func RegistOneWayHandler[T any](router *RpcReceiver, method string, call OneWayHandlerFunc[T]) {
	h := &OneWayHandler[T]{
		call:   call,
		method: method,
	}
	router.oneWayMap[method] = h
}

//----------------------------------------------------------------

type ReqResp[REQ any, RSP any] struct {
	method string
}

func (p *ReqResp[REQ, RSP]) GetMethod() string {
	return p.method
}

func newReqResp[REQ any, RSP any](method string) (*ReqResp[REQ, RSP], bool) {
	if method == "" {
		kklog.Errorf("method is empty")
		return nil, false
	}

	typeReq := reflect.TypeFor[REQ]()
	typeRsp := reflect.TypeFor[RSP]()

	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
	if gRpcManager.peers[method] != nil {
		kklog.Errorf("method %s already registered", method)
		return nil, false
	}
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
	gRpcManager.peers[method] = p
	gRpcManager.type2methodReqRsp[typeReq] = method
	gRpcManager.type2methodReqRsp[typeRsp] = method
	gRpcManager.method2typeReqRsp[method] = methodReqRsp{reqType: typeReq, rspType: typeRsp}
	return p, true
}

//----------------------------------------------------------------

type OneWay[REQ any] struct {
	method string
}

func (o *OneWay[REQ]) GetMethod() string {
	return o.method
}

func newOneWay[REQ any](method string) (*OneWay[REQ], bool) {
	if method == "" {
		kklog.Errorf("method is empty")
		return nil, false
	}

	typeReq := reflect.TypeFor[REQ]()

	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
	if gRpcManager.oneWays[method] != nil {
		kklog.Errorf("method %s already registered", method)
		return nil, false
	}
	if gRpcManager.type2methodOneWay[typeReq] != "" {
		kklog.Errorf("type %s already registered", typeReq)
		return nil, false
	}

	o := &OneWay[REQ]{
		method: method,
	}
	gRpcManager.oneWays[method] = o
	gRpcManager.type2methodOneWay[typeReq] = method
	gRpcManager.method2typeOneWay[method] = methonOneWay{reqType: typeReq}
	return o, true
}
