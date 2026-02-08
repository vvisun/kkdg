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
	// 获取消息ID
	GetMsgID() any
	// 消息回调
	OnMsg(ctx context.Context, payload []byte) ([]byte, error)
}

type RpcHandler[T any, R any] struct {
	call  RpcHandlerFunc[T, R]
	msgID any
}

func (h *RpcHandler[T, R]) GetMsgID() any {
	return h.msgID
}

func (h *RpcHandler[T, R]) OnMsg(ctx context.Context, payload []byte) ([]byte, error) {
	var data T
	var resp R
	if err := dataCodec.Unmarshal(payload, &data); err != nil {
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

//---------------------------------------------------------------

func newRpcHandler[T any, R any](method any, call RpcHandlerFunc[T, R]) *RpcHandler[T, R] {
	var handler RpcHandler[T, R]
	handler.call = call
	handler.msgID = method
	return &handler
}

func NewRpcReceiver() *RpcReceiver {
	return &RpcReceiver{
		hdMap: make(map[interface{}]IRpcHandler),
	}
}

func RegistRpcHandler[T any, R any](router *RpcReceiver, method any, call RpcHandlerFunc[T, R]) {
	h := newRpcHandler(method, call)
	router.hdMap[method] = h
}

//---------------------------------------------------------------

type RpcReceiver struct {
	hdMap map[interface{}]IRpcHandler
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

	switch fr.T {
	case FrameTypeResponse:
		kklog.Debugf("远程方法返回: %v", fr)
		return nil
	case FrameTypeTell:
		kklog.Warnf("单向调用，不应该收到响应消息: %v", fr)
		return nil
	case FrameTypeRequest:
		kklog.Debugf("收到远程方法调用请求: %v", fr)
	default:
		kklog.Warnf("收到未知类型的消息: %v", fr)
		return nil
	}

	// 处理 FrameTypeRequest 类型的请求
	rspFrame := Frame{
		T:    FrameTypeResponse,
		ID:   fr.ID,
		M:    fr.M,
		Code: 0,
		Err:  "",
	}

	method := fr.M
	h, ok := r.hdMap[method]
	if !ok || h == nil {
		rspFrame.Code = 1
		rspFrame.Err = "未找到远程方法" + method
		rspBB, err := EncodeFailedResponse(&rspFrame)
		if err != nil {
			kklog.Errorf("encode failed response: %v", err)
			return nil
		}
		return rspBB
	}

	respBytes, err := h.OnMsg(context.Background(), fr.P)
	if err != nil {
		rspFrame.Code = 1
		rspFrame.Err = "远程方法执行失败: " + err.Error()
		rspBB, err := EncodeFailedResponse(&rspFrame)
		if err != nil {
			kklog.Errorf("encode failed response: %v", err)
			return nil
		}
		return rspBB
	}

	// encode response
	rspBB, err := EncodeRpcFrame(FrameTypeResponse, fr.ID, method, respBytes)
	if err != nil {
		return nil
	}
	//喂给上层函数发送回执
	return rspBB
}
