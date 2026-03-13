package kkrpc

import (
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
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
	type2methodReqRsp map[reflect.Type]string
	method2typeReqRsp map[string]methodReqRsp
	type2methodOneWay map[reflect.Type]string
	method2typeOneWay map[string]methodOneWay
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
		type2methodReqRsp: make(map[reflect.Type]string),
		method2typeReqRsp: make(map[string]methodReqRsp),
		type2methodOneWay: make(map[reflect.Type]string),
		method2typeOneWay: make(map[string]methodOneWay),
	}
}
