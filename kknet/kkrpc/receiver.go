package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type RpcHandlerFunc[T any, R any] func(ctx context.Context, msg *T, resp *R) error

// 消息接收器
type IRpcHandler interface {
	GetMsgID() any                                              // 获取消息ID
	OnMsg(ctx context.Context, msgBytes []byte) ([]byte, error) // 消息回调
}

type RpcHandler[T any, R any] struct {
	call  RpcHandlerFunc[T, R]
	msgID any
}

func (h *RpcHandler[T, R]) GetMsgID() any {
	return h.msgID
}

func (h *RpcHandler[T, R]) OnMsg(ctx context.Context, msgBytes []byte) ([]byte, error) {
	var data T
	var resp R
	if err := dataCodec.Unmarshal(msgBytes, &data); err != nil {
		return nil, err
	}
	err := h.call(ctx, &data, &resp)
	if err != nil {
		return nil, err
	}

	respBytes, err := dataCodec.Marshal(&resp)
	if err != nil {
		return nil, err
	}
	return respBytes, nil
}

func newRpcHandler[T any, R any](method any, call RpcHandlerFunc[T, R]) *RpcHandler[T, R] {
	var handler RpcHandler[T, R]
	handler.call = call
	handler.msgID = method
	return &handler
}

//---------------------------------------------------------------

type RpcReceiver struct {
	m map[interface{}]IRpcHandler
}

func (r *RpcReceiver) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) *kkbuffer.ByteBuffer {
	frameBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		kkbuffer.Put(data)
		return nil
	}
	var fr Frame
	if err := rpcCodec.Unmarshal(frameBytes, &fr); err != nil {
		kkbuffer.Put(data)
		return nil
	}
	kkbuffer.Put(data)

	method := fr.M
	h, ok := r.m[method]
	if !ok || h == nil {
		return nil
	}

	respBytes, err := h.OnMsg(context.Background(), fr.P)
	if err != nil {
		return nil
	}

	if fr.T == FrameTypeResponse {
		kklog.Debugf("response frame: %v", fr)
		return nil
	}

	if fr.T != FrameTypeRequest {
		return nil // only request need response
	}

	// encode response
	rspBB, err := EncodeRpcFrame(FrameTypeResponse, fr.ID, method, respBytes)
	if err != nil {
		return nil
	}
	return rspBB
}

func NewRpcReceiver() *RpcReceiver {
	return &RpcReceiver{
		m: make(map[interface{}]IRpcHandler),
	}
}

func RegistRpcHandler[T any, R any](router *RpcReceiver, method any, call RpcHandlerFunc[T, R]) {
	h := newRpcHandler(method, call)
	router.m[method] = h
}
