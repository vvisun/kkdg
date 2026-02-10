package kkrpc

import (
	"context"
	"reflect"
	"sync"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type ISender interface {
	SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error
	getPending() *pendingMap
}

type RpcInvoker[T any, R any] struct {
	sender ISender
}

func NewRpcInvoker[T any, R any](sender ISender) RpcInvoker[T, R] {
	return RpcInvoker[T, R]{
		sender: sender,
	}
}

// Invoke 同步调用，阻塞直到收到响应或 ctx 取消/超时
func (i RpcInvoker[T, R]) Invoke(ctx context.Context, method string, req *T, opts CallConfig, rsp *R) error {
	if i.sender.getPending().IsClosed() {
		return ErrConnClosed
	}
	if ctx == nil {
		ctx = context.Background()
	}
	pending := i.sender.getPending()
	reqId := genReqId()
	ch, ok := pending.addCh(reqId)
	if !ok {
		return ErrConnClosed
	}
	defer pending.delCh(reqId)

	bb, err := EncodeRpcFrame(FrameTypeRequest, reqId, method, req)
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	if err = i.sender.SendBuffer(0, bb); err != nil {
		return err
	}

	timeout := opts.timeout
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d > 0 && (timeout <= 0 || d < timeout) {
			timeout = d
		}
	}
	var timer *time.Timer
	if timeout > 0 {
		timer = time.NewTimer(timeout)
		defer timer.Stop()
	}

	doReturn := func(fr Frame) error {
		if fr.T != FrameTypeResponse {
			return ErrInvalidFrameType
		}
		err := ErrRpc(fr.Code, fr.Err)
		if err != nil {
			return err
		}
		return payloadCodec.Unmarshal(fr.P, rsp)
	}

	if timer != nil {
		select {
		case fr := <-ch:
			return doReturn(fr)
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return ErrTimeout
		}
	}
	select {
	case fr := <-ch:
		return doReturn(fr)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 异步调用（非阻塞等待结果）
func (i RpcInvoker[T, R]) InvokeAsync(ctx context.Context, method string, req *T, opts CallConfig, callback func(rsp *R, err error)) error {
	if i.sender.getPending().IsClosed() {
		return ErrConnClosed
	}
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
	var respInfo R
	i.sender.getPending().addCallback(reqId, func(fr Frame) {
		if fr.ID != reqId || fr.T != FrameTypeResponse {
			return
		}
		i.sender.getPending().delCallback(reqId)

		err := ErrRpc(fr.Code, fr.Err)
		if err != nil {
			callback(nil, err)
			return
		}
		err = payloadCodec.Unmarshal(fr.P, &respInfo)
		if err != nil {
			callback(nil, err)
			return
		}
		callback(&respInfo, nil)
	})
	err = i.sender.SendBuffer(0, bb)
	if err != nil {
		return err
	}
	return nil
}

// 无响应调用（没有结果，单向调用）
func (i RpcInvoker[T, R]) InvokeNR(ctx context.Context, method string, req *T, opts CallConfig) error {
	if i.sender.getPending().IsClosed() {
		return ErrConnClosed
	}
	// if !CheckReqResp(req, rsp) {
	// 	kklog.Errorf("req resp type not match")
	// 	return ErrInvalidReqResp
	// }
	bb, err := EncodeRpcFrame(FrameTypeOneway, 0, method, req)
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	err = i.sender.SendBuffer(0, bb)
	if err != nil {
		return err
	}
	return nil
}

//----------------------------------------------------------------

type methodType struct {
	reqType reflect.Type
	rspType reflect.Type
}

type rpcManager struct {
	mu          sync.Mutex
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
	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
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
	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
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
