package kkrpc

import (
	"fmt"
	"reflect"
	"sync"

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

func NewRpcReceiver() *RpcReceiver {
	return &RpcReceiver{
		hdMap:     make(map[string]IRpcHandler),
		oneWayMap: make(map[string]IOneWayHandler),
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

type (
	methodReqRsp struct {
		reqType reflect.Type
		rspType reflect.Type
	}
	methonOneWay struct {
		reqType reflect.Type
	}
)

type rpcManager struct {
	mu                sync.Mutex
	peers             map[string]interface{}
	oneWays           map[string]interface{}
	type2methodReqRsp map[reflect.Type]string
	method2typeReqRsp map[string]methodReqRsp
	type2methodOneWay map[reflect.Type]string
	method2typeOneWay map[string]methonOneWay
}

func (rm *rpcManager) getMethod(msg any) string {
	tp := reflect.TypeOf(msg)
	method, ok := rm.type2methodOneWay[tp]
	if ok {
		return method
	}
	method, ok = rm.type2methodReqRsp[tp]
	if ok {
		return method
	}
	return ""
}

func newRpcManager() *rpcManager {
	return &rpcManager{
		peers:             make(map[string]interface{}),
		oneWays:           make(map[string]interface{}),
		type2methodReqRsp: make(map[reflect.Type]string),
		method2typeReqRsp: make(map[string]methodReqRsp),
		type2methodOneWay: make(map[reflect.Type]string),
		method2typeOneWay: make(map[string]methonOneWay),
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
	gRpcManager.method2typeReqRsp = make(map[string]methodReqRsp)
	gRpcManager.type2methodOneWay = make(map[reflect.Type]string)
	gRpcManager.method2typeOneWay = make(map[string]methonOneWay)
}

// verifyReqRespMethod verifies at init that REQ/RSP types are registered for method. No runtime reflect on hot path.
func verifyReqRespMethod[REQ any, RSP any]() (string, bool) {
	typeReq := reflect.TypeFor[REQ]()
	typeRsp := reflect.TypeFor[RSP]()
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
	typeReq := reflect.TypeFor[REQ]()
	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
	m, ok := gRpcManager.type2methodOneWay[typeReq]
	return m, ok
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
