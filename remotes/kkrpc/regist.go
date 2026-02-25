package kkrpc

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/utils/kklog"
)

// RegistReqRspHandler 注册请求响应方法 handler
func RegistReqRspHandler[T any, R any](router *RpcReceiver, method string, call ReqRspHandlerFunc[T, R]) {
	h := newReqRspHandler(method, call)
	router.hdMap[method] = h
}

// RegistOneWayHandler 注册单向消息方法 handler
func RegistOneWayHandler[T any](router *RpcReceiver, method string, call OneWayHandlerFunc[T]) {
	h := newOneWayHandler(method, call)
	router.oneWayMap[method] = h
}

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

func newReqRspHandler[T any, R any](method string, call ReqRspHandlerFunc[T, R]) *ReqRspHandler[T, R] {
	return &ReqRspHandler[T, R]{
		call:   call,
		method: method,
	}
}

func newOneWayHandler[T any](method string, call OneWayHandlerFunc[T]) *OneWayHandler[T] {
	return &OneWayHandler[T]{
		call:   call,
		method: method,
	}
}

func NewRpcReceiver() *RpcReceiver {
	return &RpcReceiver{
		hdMap:     make(map[string]IRpcHandler),
		oneWayMap: make(map[string]IOneWayHandler),
	}
}

//----------------------------------------------------------------

type methodType struct {
	reqType reflect.Type
	rspType reflect.Type
}

type rpcManager struct {
	mu                sync.Mutex
	peers             map[string]interface{}
	oneWays           map[string]interface{}
	type2methodReqRsp map[reflect.Type]string
	method2typeReqRsp map[string]methodType
	type2methodOneWay map[reflect.Type]string
	method2typeOneWay map[string]methodType
}

func newRpcManager() *rpcManager {
	return &rpcManager{
		peers:             make(map[string]interface{}),
		oneWays:           make(map[string]interface{}),
		type2methodReqRsp: make(map[reflect.Type]string),
		method2typeReqRsp: make(map[string]methodType),
		type2methodOneWay: make(map[reflect.Type]string),
		method2typeOneWay: make(map[string]methodType),
	}
}

var (
	gRpcManager *rpcManager = newRpcManager()
)

// ClearRpcManagerForTest 清空 gRpcManager 中所有注册信息。
// 仅用于测试场景，便于多测试重复注册。生产环境请勿调用。
func ClearRpcManagerForTest() {
	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
	gRpcManager.peers = make(map[string]interface{})
	gRpcManager.oneWays = make(map[string]interface{})
	gRpcManager.type2methodReqRsp = make(map[reflect.Type]string)
	gRpcManager.method2typeReqRsp = make(map[string]methodType)
	gRpcManager.type2methodOneWay = make(map[reflect.Type]string)
	gRpcManager.method2typeOneWay = make(map[string]methodType)
}

func CheckReqResp[REQ any, RSP any](req *REQ, rsp *RSP) bool {
	typeReq := reflect.TypeOf(req)
	methodReq, ok := gRpcManager.type2methodReqRsp[typeReq]
	if !ok {
		return false
	}
	typeRsp := reflect.TypeOf(rsp)
	methodRsp, ok := gRpcManager.type2methodReqRsp[typeRsp]
	if !ok {
		return false
	}
	return methodReq == methodRsp
}

func CheckOneWay[REQ any](req *REQ) bool {
	typeReq := reflect.TypeOf(req)
	method, ok := gRpcManager.type2methodOneWay[typeReq]
	if !ok {
		return false
	}
	return method != ""
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

	var vReq REQ
	var vRSP RSP
	typeReq := reflect.TypeOf(&vReq)
	typeRsp := reflect.TypeOf(&vRSP)

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
	gRpcManager.method2typeReqRsp[method] = methodType{reqType: typeReq, rspType: typeRsp}
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

	var vReq REQ
	typeReq := reflect.TypeOf(&vReq)

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
	gRpcManager.method2typeOneWay[method] = methodType{reqType: typeReq}
	return o, true
}
