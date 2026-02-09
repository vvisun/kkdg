package kkrpc

import (
	"context"
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type SendFunc func(data *kkbuffer.ByteBuffer) error

type RpcInvoker[T any, R any] struct {
	c *Client
}

func (i RpcInvoker[T, R]) Invoke(ctx context.Context, method string, req *T, opts CallConfig, rsp *R) error {
	// if !CheckReqResp(req, rsp) {
	// 	kklog.Errorf("req resp type not match")
	// 	return ErrInvalidReqResp
	// }
	reqId := genReqId()
	bb, err := EncodeRpcFrame(FrameTypeRequest, reqId, method, req)
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	i.c.pending[reqId] = func(fr Frame) {
		if fr.ID != reqId || fr.T != FrameTypeResponse {
			return
		}
		payloadCodec.Unmarshal(fr.P, rsp)
		delete(i.c.pending, reqId)
		kklog.Infof("recv response: %v, %v, %v, %v", fr.M, fr.ID, fr.Code, rsp)
	}
	err = i.c.SendBuffer(bb)
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

type methodType struct {
	reqType reflect.Type
	rspType reflect.Type
}

type rpcManager struct {
	peers       map[string]interface{}
	oneWays     map[string]interface{}
	type2method map[reflect.Type]string
	method2type map[string]methodType
}

func newRpcManager() *rpcManager {
	return &rpcManager{
		peers:       make(map[string]interface{}),
		oneWays:     make(map[string]interface{}),
		type2method: make(map[reflect.Type]string),
		method2type: make(map[string]methodType),
	}
}

var (
	gRpcManager    = newRpcManager()
	onceRpcManager = sync.Once{}
)

func DefaultRpcManager() *rpcManager {
	onceRpcManager.Do(func() {
		gRpcManager = newRpcManager()
	})
	return gRpcManager
}

func CheckReqResp[REQ any, RSP any](req *REQ, rsp *RSP) bool {
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

func CheckOneWay[REQ any](req *REQ) bool {
	typeReq := reflect.TypeOf(req)
	methodReq, ok := gRpcManager.type2method[typeReq]
	if !ok {
		return false
	}
	return methodReq != ""
}

//----------------------------------------------------------------

type ReqResp[REQ any, RSP any] struct {
	method string
}

func (p *ReqResp[REQ, RSP]) GetMethod() string {
	return p.method
}

func newReqResp[REQ any, RSP any](method string) *ReqResp[REQ, RSP] {
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
	p := &ReqResp[REQ, RSP]{
		method: method,
	}
	var vReq REQ
	var vRSP RSP
	typeReq := reflect.TypeOf(&vReq)
	typeRsp := reflect.TypeOf(&vRSP)
	gRpcManager.peers[method] = p
	gRpcManager.type2method[typeReq] = method
	gRpcManager.type2method[typeRsp] = method
	gRpcManager.method2type[method] = methodType{reqType: typeReq, rspType: typeRsp}
	return p
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
	gRpcManager.method2type[method] = methodType{reqType: typeReq, rspType: nil}
	return o
}
