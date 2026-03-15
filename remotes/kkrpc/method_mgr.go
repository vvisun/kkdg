package kkrpc

import (
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type (
	methodReqRsp struct {
		reqType reflect.Type
		rspType reflect.Type
	}
	methodOneWay struct {
		reqType reflect.Type
	}
)

type MethodManager struct {
	streamTool        kkpacket.IPacket
	frameCodec        kkcodec.ICodec
	payloadCodec      kkcodec.ICodec
	mu                sync.Mutex
	method2typeReqRsp map[string]methodReqRsp
	type2methodOneWay map[reflect.Type]string
	method2typeOneWay map[string]methodOneWay
}

func (rm *MethodManager) getMethodOneway(msg any) string {
	tp := reflect.TypeOf(msg)
	method, ok := rm.type2methodOneWay[tp]
	if ok {
		return method
	}
	return ""
}

func (rm *MethodManager) autoMethodName(msgs ...any) string {
	if len(msgs) == 0 {
		return ""
	}
	if len(msgs) == 1 {
		return xreflect.ObjectTypeName(msgs[0])
	}
	name := ""
	for i, msg := range msgs {
		if i != 0 {
			name += "_"
		}
		name += xreflect.ObjectTypeName(msg)
	}
	return name
}

func (rm *MethodManager) getMethodReqRsp(req any, rsp any) string {
	nameReq := xreflect.ObjectTypeName(req)
	nameRsp := xreflect.ObjectTypeName(rsp)
	fullName := nameReq + "_" + nameRsp
	_, ok := rm.method2typeReqRsp[fullName]
	if ok {
		return fullName
	}
	return ""
}

func NewMethodManager(streamTool kkpacket.IPacket, frameCodec kkcodec.ICodec, payloadCodec kkcodec.ICodec) *MethodManager {
	if streamTool == nil {
		kklog.Errorf("streamTool is nil, use default streamTool")
		streamTool = kkpacket.DefaultStreamPacket()
	}
	if frameCodec == nil {
		kklog.Errorf("frameCodec is nil, use default frameCodec: %s", "msgpack")
		frameCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
	if payloadCodec == nil {
		kklog.Errorf("payloadCodec is nil, use default payloadCodec: %s", "msgpack")
		payloadCodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
	return &MethodManager{
		streamTool:        streamTool,
		frameCodec:        frameCodec,
		payloadCodec:      payloadCodec,
		method2typeReqRsp: make(map[string]methodReqRsp),
		type2methodOneWay: make(map[reflect.Type]string),
		method2typeOneWay: make(map[string]methodOneWay),
	}
}

//----------------------------------------------------------------

func newReqResp[REQ any, RSP any](method string, methodMgr *MethodManager) error {
	var vReq REQ
	var vRsp RSP
	var pReq *REQ
	var pRsp *RSP

	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()

	fullName1 := methodMgr.autoMethodName(vReq, vRsp)
	if _, ok := methodMgr.method2typeReqRsp[fullName1]; ok {
		return kkerrors.ErrRpcMethodAlreadyRegistered
	}
	fullName2 := methodMgr.autoMethodName(pReq, pRsp)
	if _, ok := methodMgr.method2typeReqRsp[fullName2]; ok {
		return kkerrors.ErrRpcMethodAlreadyRegistered
	}
	fullName3 := methodMgr.autoMethodName(vReq, pRsp)
	if _, ok := methodMgr.method2typeReqRsp[fullName3]; ok {
		return kkerrors.ErrRpcMethodAlreadyRegistered
	}
	fullName4 := methodMgr.autoMethodName(pReq, vRsp)
	if _, ok := methodMgr.method2typeReqRsp[fullName4]; ok {
		return kkerrors.ErrRpcMethodAlreadyRegistered
	}

	typeReq := reflect.TypeFor[*REQ]()
	typeRsp := reflect.TypeFor[*RSP]()
	typeReqValue := reflect.TypeFor[REQ]()
	typeRspValue := reflect.TypeFor[RSP]()
	methodMgr.method2typeReqRsp[fullName1] = methodReqRsp{reqType: typeReq, rspType: typeRsp}
	methodMgr.method2typeReqRsp[fullName2] = methodReqRsp{reqType: typeReqValue, rspType: typeRspValue}
	methodMgr.method2typeReqRsp[fullName3] = methodReqRsp{reqType: typeReq, rspType: typeRspValue}
	methodMgr.method2typeReqRsp[fullName4] = methodReqRsp{reqType: typeReqValue, rspType: typeRsp}

	if method != "" {
		if method != fullName1 && method != fullName2 && method != fullName3 && method != fullName4 {
			methodMgr.method2typeReqRsp[method] = methodReqRsp{reqType: typeReq, rspType: typeRsp}
		}
	}

	return nil
}

func newOneWay[REQ any](method string, methodMgr *MethodManager) error {
	if method == "" {
		var v *REQ
		method = methodMgr.autoMethodName(v)
	}
	if method == "" {
		var v *REQ
		kklog.Errorf("invalid oneway type %s", xreflect.ObjectTypeName(v))
		return kkerrors.ErrRpcInvalidOneWay
	}

	typeReq := reflect.TypeFor[*REQ]()

	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()

	if methodMgr.type2methodOneWay[typeReq] != "" && methodMgr.type2methodOneWay[typeReq] != method {
		kklog.Errorf("type %s already registered", typeReq)
		return kkerrors.ErrRpcMethodAlreadyRegistered
	}

	methodMgr.type2methodOneWay[typeReq] = method
	methodMgr.method2typeOneWay[method] = methodOneWay{reqType: typeReq}

	typeReqValue := reflect.TypeFor[REQ]()
	methodMgr.type2methodOneWay[typeReqValue] = method

	return nil
}

// verifyReqRespMethod verifies at init that REQ/RSP types are registered for method. No runtime reflect on hot path.
func verifyReqRespMethod[REQ any, RSP any](methodMgr *MethodManager) (string, bool) {
	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()
	var pReq *REQ
	var pRsp *RSP
	method := methodMgr.getMethodReqRsp(pReq, pRsp)
	if method != "" {
		return method, true
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
