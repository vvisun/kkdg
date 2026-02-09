package kkrpc

import (
	"context"
	"reflect"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type SendFunc func(data *kkbuffer.ByteBuffer) error

type RpcInvoker[T any, R any] struct {
	sendFunc SendFunc
}

func (i RpcInvoker[T, R]) Invoke(ctx context.Context, method string, req *T, opts CallConfig, rsp *R) error {
	// if !CheckPeer(req, rsp) {
	// 	kklog.Errorf("peer not found")
	// 	return ErrInvalidPeer
	// }
	bb, err := EncodeRpcFrame(FrameTypeRequest, genReqId(), method, req)
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	err = i.sendFunc(bb)
	if err != nil {
		return err
	}
	return nil
}

func (i RpcInvoker[T, R]) InvokeAsync(ctx context.Context, method string, req *T, opts CallConfig, callback func(rsp *R)) error {
	return nil
}

func (i RpcInvoker[T, R]) InvokeNR(ctx context.Context, method string, req *T, opts CallConfig) error {
	return nil
}

//----------------------------------------------------------------

type rpcManager struct {
	peers       map[string]interface{}
	oneWays     map[string]interface{}
	type2method map[reflect.Type]string
}

func newRpcManager() *rpcManager {
	return &rpcManager{
		peers:       make(map[string]interface{}),
		oneWays:     make(map[string]interface{}),
		type2method: make(map[reflect.Type]string),
	}
}

var gRpcManager = newRpcManager()

func DefaultRpcManager() *rpcManager {
	return gRpcManager
}

//----------------------------------------------------------------

type RpcPeer[REQ any, RSP any] struct {
	method string
}

func (p *RpcPeer[REQ, RSP]) GetMethod() string {
	return p.method
}

func newRpcPeer[REQ any, RSP any](method string) *RpcPeer[REQ, RSP] {
	if method == "" {
		kklog.Errorf("method is empty")
		return nil
	}
	if gRpcManager.peers[method] != nil {
		kklog.Errorf("method %s already registered", method)
		return nil
	}
	if gRpcManager.type2method[reflect.TypeOf((*REQ)(nil))] != "" {
		kklog.Errorf("type %s already registered", reflect.TypeOf((*REQ)(nil)))
		return nil
	}
	if gRpcManager.type2method[reflect.TypeOf((*RSP)(nil))] != "" {
		kklog.Errorf("type %s already registered", reflect.TypeOf((*RSP)(nil)))
		return nil
	}
	p := &RpcPeer[REQ, RSP]{
		method: method,
	}
	var vReq REQ
	var vRSP RSP
	typeReq := reflect.TypeOf(&vReq)
	typeRsp := reflect.TypeOf(&vRSP)
	gRpcManager.peers[method] = p
	gRpcManager.type2method[typeReq] = method
	gRpcManager.type2method[typeRsp] = method
	return p
}

func CheckPeer[REQ any, RSP any](req *REQ, rsp *RSP) bool {
	typeReq := reflect.TypeOf(req)
	methodReq, ok := gRpcManager.type2method[typeReq]
	if !ok {
		return false
	}
	typeRsp := reflect.TypeOf(rsp)
	methodRsp, ok := gRpcManager.type2method[typeRsp]
	if !ok {
		return false
	}
	return methodReq == methodRsp
}

//----------------------------------------------------------------

type OneWay[REQ any] struct {
	method string
}

func (o *OneWay[REQ]) GetMethod() string {
	return o.method
}

func newOneWay[REQ any](method string) *OneWay[REQ] {
	if method == "" {
		kklog.Errorf("method is empty")
		return nil
	}
	if gRpcManager.oneWays[method] != nil {
		kklog.Errorf("method %s already registered", method)
		return nil
	}
	if gRpcManager.type2method[reflect.TypeOf((*REQ)(nil))] != "" {
		kklog.Errorf("type %s already registered", reflect.TypeOf((*REQ)(nil)))
		return nil
	}
	o := &OneWay[REQ]{
		method: method,
	}
	var vReq REQ
	typeReq := reflect.TypeOf(&vReq)
	gRpcManager.oneWays[method] = o
	gRpcManager.type2method[typeReq] = method
	return o
}
