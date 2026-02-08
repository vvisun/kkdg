package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type RpcHandlerFunc[T any] func(ctx context.Context, msg *T) error

// 消息接收器
type IRpcHandler interface {
	GetMsgID() any                                    // 获取消息ID
	OnMsg(ctx context.Context, msgBytes []byte) error // 消息回调
}

type RpcHandler[T any] struct {
	call  RpcHandlerFunc[T]
	msgID any
}

func (h *RpcHandler[T]) GetMsgID() any {
	return h.msgID
}

func (h *RpcHandler[T]) OnMsg(ctx context.Context, msgBytes []byte) error {
	var data T
	if err := dataCodec.Unmarshal(msgBytes, &data); err != nil {
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

func (r *RpcRouter) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		kkbuffer.Put(data)
		return
	}
	var fr Frame
	if err := rpcCodec.Unmarshal(msgBytes, &fr); err != nil {
		kkbuffer.Put(data)
		return
	}

	switch fr.T {
	case FrameTypeRequest:

	case FrameTypeResponse:

	case FrameTypeTell:

	}

	method := fr.M
	h, ok := r.m[method]
	if !ok || h == nil {
		kkbuffer.Put(data)
		return
	}
	err = h.OnMsg(context.Background(), fr.P)
	kkbuffer.Put(data)
	if err != nil {
		kklog.Errorf("server handler on msg: %v", err)
	}
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
