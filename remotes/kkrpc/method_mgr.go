package kkrpc

import (
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
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

type MethodManager struct {
	streamTool        kkpacket.IPacket
	frameCodec        kkcodec.ICodec
	payloadCodec      kkcodec.ICodec
	mu                sync.Mutex
	type2methodReqRsp map[reflect.Type]string
	method2typeReqRsp map[string]methodReqRsp
	type2methodOneWay map[reflect.Type]string
	method2typeOneWay map[string]methonOneWay
}

func (rm *MethodManager) getMethod(msg any) string {
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

func NewMethodManager() *MethodManager {
	return &MethodManager{
		type2methodReqRsp: make(map[reflect.Type]string),
		method2typeReqRsp: make(map[string]methodReqRsp),
		type2methodOneWay: make(map[reflect.Type]string),
		method2typeOneWay: make(map[string]methonOneWay),
	}
}
