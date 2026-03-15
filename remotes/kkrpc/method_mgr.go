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
		reqType        reflect.Type
		rspType        reflect.Type
		selfDefineName string //自定义方法名
	}
	methodOneWay struct {
		reqType        reflect.Type
		selfDefineName string //自定义方法名
	}
)

func getObjectName(obj any) string {
	return xreflect.GetStructName(obj)
}

type MethodManager struct {
	streamTool        kkpacket.IPacket
	frameCodec        kkcodec.ICodec
	payloadCodec      kkcodec.ICodec
	mu                sync.Mutex
	method2typeReqRsp map[string]methodReqRsp
	method2typeOneWay map[string]methodOneWay
}

func (rm *MethodManager) autoMethodName(msgs ...any) string {
	if len(msgs) == 0 {
		return ""
	}
	if len(msgs) == 1 {
		return getObjectName(msgs[0])
	}
	name := ""
	for i, msg := range msgs {
		if i != 0 {
			name += "_"
		}
		name += getObjectName(msg)
	}
	return name
}

func (rm *MethodManager) getMethodReqRsp(req any, rsp any) (string, string) {
	fullName := rm.autoMethodName(req, rsp)
	info, ok := rm.method2typeReqRsp[fullName]
	if ok {
		return fullName, info.selfDefineName
	}
	return "", ""
}

func (rm *MethodManager) getMethodOneway(msg any) (string, string) {
	fullName := rm.autoMethodName(msg)
	info, ok := rm.method2typeOneWay[fullName]
	if ok {
		return fullName, info.selfDefineName
	}
	return "", ""
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

	selfDefineName := ""
	if method != "" {
		if method != fullName1 && method != fullName2 && method != fullName3 && method != fullName4 {
			selfDefineName = method
		}
	}

	typeReq := reflect.TypeFor[*REQ]()
	typeRsp := reflect.TypeFor[*RSP]()
	typeReqValue := reflect.TypeFor[REQ]()
	typeRspValue := reflect.TypeFor[RSP]()

	methodMgr.method2typeReqRsp[fullName2] = methodReqRsp{reqType: typeReqValue, rspType: typeRspValue, selfDefineName: selfDefineName}
	methodMgr.method2typeReqRsp[fullName3] = methodReqRsp{reqType: typeReq, rspType: typeRspValue, selfDefineName: selfDefineName}
	methodMgr.method2typeReqRsp[fullName4] = methodReqRsp{reqType: typeReqValue, rspType: typeRsp, selfDefineName: selfDefineName}
	methodMgr.method2typeReqRsp[fullName1] = methodReqRsp{reqType: typeReq, rspType: typeRsp, selfDefineName: selfDefineName}

	if selfDefineName != "" {
		methodMgr.method2typeReqRsp[selfDefineName] = methodReqRsp{
			reqType:        typeReq,
			rspType:        typeRsp,
			selfDefineName: selfDefineName,
		}
	}

	return nil
}

func newOneWay[REQ any](method string, methodMgr *MethodManager) error {
	var vReq REQ
	var pReq *REQ

	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()

	fullName1 := methodMgr.autoMethodName(vReq)
	if _, ok := methodMgr.method2typeReqRsp[fullName1]; ok {
		return kkerrors.ErrRpcMethodAlreadyRegistered
	}
	fullName2 := methodMgr.autoMethodName(pReq)
	if _, ok := methodMgr.method2typeReqRsp[fullName2]; ok {
		return kkerrors.ErrRpcMethodAlreadyRegistered
	}

	selfDefineName := ""
	if method != "" {
		if method != fullName1 && method != fullName2 {
			selfDefineName = method
		}
	}

	typeReq := reflect.TypeFor[*REQ]()
	typeReqValue := reflect.TypeFor[REQ]()

	methodMgr.method2typeOneWay[fullName2] = methodOneWay{reqType: typeReqValue, selfDefineName: selfDefineName}
	methodMgr.method2typeOneWay[fullName1] = methodOneWay{reqType: typeReq, selfDefineName: selfDefineName}

	if selfDefineName != "" {
		methodMgr.method2typeOneWay[selfDefineName] = methodOneWay{
			reqType:        typeReq,
			selfDefineName: selfDefineName,
		}
	}

	return nil
}

// verifyReqRespMethod verifies at init that REQ/RSP types are registered for method. No runtime reflect on hot path.
func verifyReqRespMethod[REQ any, RSP any](methodMgr *MethodManager) (string, bool) {
	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()
	var pReq *REQ
	var pRsp *RSP
	fullName, _ := methodMgr.getMethodReqRsp(pReq, pRsp)
	if fullName != "" {
		return fullName, true
	}
	return "", false
}

// verifyOneWayMethod verifies at init that REQ type is registered for method. No runtime reflect on hot path.
func verifyOneWayMethod[REQ any](methodMgr *MethodManager) (string, bool) {
	methodMgr.mu.Lock()
	defer methodMgr.mu.Unlock()
	var pReq *REQ
	fullName, _ := methodMgr.getMethodOneway(pReq)
	if fullName != "" {
		return fullName, true
	}
	return "", false
}
