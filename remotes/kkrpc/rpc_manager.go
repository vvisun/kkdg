package kkrpc

import (
	"reflect"
	"sync"
)

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
