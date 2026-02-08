package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type RpcHandlerFunc[T any] func(ctx context.Context, msg *T) error

// 消息接收器
type IRpcHandler interface {
	GetMsgID() any                               // 获取消息ID
	OnMsg(ctx context.Context, msg []byte) error // 消息回调
}

type RpcHandler[T any] struct {
	call  RpcHandlerFunc[T]
	msgID any
}

func (h *RpcHandler[T]) GetMsgID() any {
	return h.msgID
}

func (h *RpcHandler[T]) OnMsg(ctx context.Context, msg []byte) error {
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(msg)
	if err != nil {
		return err
	}
	var frame Frame
	if err := rpcCodec.Unmarshal(msgBytes, &frame); err != nil {
		return err
	}
	var data T
	if err := dataCodec.Unmarshal(frame.P, &data); err != nil {
		return err
	}
	return h.call(ctx, &data)
}

func newRpcHandler[T any](method any, call RpcHandlerFunc[T]) *RpcHandler[T] {
	var handler RpcHandler[T]
	handler.call = call
	handler.msgID = method
	return &handler
}

//---------------------------------------------------------------

type RpcRouter struct {
	m map[interface{}]IRpcHandler
}

func (r *RpcRouter) OnMsg(ctx context.Context, method any, msg []byte) error {
	h, ok := r.m[method]
	if !ok || h == nil {
		return ErrMethodNotFound
	}
	return h.OnMsg(ctx, msg)
}

func NewRpcRouter() *RpcRouter {
	return &RpcRouter{
		m: make(map[interface{}]IRpcHandler),
	}
}

func RegistRpcHandler[T any](router *RpcRouter, method any, call RpcHandlerFunc[T]) {
	h := newRpcHandler(method, call)
	router.m[method] = h
}

//----------------------------------------------------------------

type IMsgPeer[REQ any, RSP any] struct {
	method string
}

func (p *IMsgPeer[REQ, RSP]) GetMethod() string {
	return p.method
}

func NewMsgPeer[REQ any, RSP any](method string) *IMsgPeer[REQ, RSP] {
	return &IMsgPeer[REQ, RSP]{
		method: method,
	}
}

func (p *IMsgPeer[REQ, RSP]) EncodeReq(req *REQ) (*kkbuffer.ByteBuffer, error) {
	bb, err := EncodeRpcFrame(FrameTypeRequest, genReqId(), p.method, req)
	if err != nil {
		return nil, err
	}
	return bb, nil
}

func (p *IMsgPeer[REQ, RSP]) DecodeReq(bb *kkbuffer.ByteBuffer) (*REQ, error) {
	info, err := DecodeRpcFrame[REQ](bb)
	if err != nil {
		return nil, err
	}
	return info, nil
}

func (p *IMsgPeer[REQ, RSP]) EncodeRsp(rsp *RSP) (*kkbuffer.ByteBuffer, error) {
	bb, err := EncodeRpcFrame(FrameTypeResponse, genReqId(), p.method, rsp)
	if err != nil {
		return nil, err
	}
	return bb, nil
}

func (p *IMsgPeer[REQ, RSP]) DecodeRsp(bb *kkbuffer.ByteBuffer) (*RSP, error) {
	info, err := DecodeRpcFrame[RSP](bb)
	if err != nil {
		return nil, err
	}
	return info, nil
}
