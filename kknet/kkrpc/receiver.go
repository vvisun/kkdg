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
	// GetMethod 返回 RPC 方法名
	GetMethod() string
	// 消息回调
	OnMsg(ctx context.Context, payload []byte, frameType FrameType) ([]byte, error)
}

type RpcHandler[T any, R any] struct {
	call   RpcHandlerFunc[T, R]
	method string
}

func (h *RpcHandler[T, R]) GetMethod() string {
	return h.method
}

func (h *RpcHandler[T, R]) OnMsg(ctx context.Context, payload []byte, frameType FrameType) ([]byte, error) {
	var data T
	var resp R
	if err := payloadCodec.Unmarshal(payload, &data); err != nil {
		return nil, err
	}
	// kklog.Debugf("收到远程方法调用请求: %v", data)
	err := h.call(ctx, &data, &resp)
	if err != nil {
		return nil, err
	}

	if frameType == FrameTypeOneway {
		return nil, nil
	}

	respBytes, err := payloadCodec.Marshal(&resp)
	if err != nil {
		return nil, err
	}
	return respBytes, nil
}

//---------------------------------------------------------------

type RpcReceiver struct {
	hdMap map[string]IRpcHandler
}

func (r *RpcReceiver) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer, pending *pendingMap) *kkbuffer.ByteBuffer {
	frameBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		kkbuffer.Put(data)
		return nil
	}
	var fr Frame
	err = frameCodec.Unmarshal(frameBytes, &fr)
	kkbuffer.Put(data)
	if err != nil {
		kklog.Debugf("failed to unmarshal frame: %v", err)
		return nil
	}

	switch fr.T {
	case FrameTypeOneway, FrameTypeRequest:
		// 处理 FrameTypeRequest/FrameTypeTell 类型的请求
	case FrameTypeResponse:
		// 收到了远程方法的返回结果（同步 Invoke 或异步 callback）
		if pending != nil {
			pending.deliver(fr.ID, fr)
		}
		return nil
	default:
		kklog.Debugf("收到未知类型的消息: %v, %v", fr.T, fr.M)
		return nil
	}

	// 处理 FrameTypeRequest/FrameTypeTell 类型的请求
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
		if fr.T == FrameTypeOneway {
			return nil
		}
		rspFrame.Code = 1
		rspFrame.Err = "未找到远程方法" + method
		rspBB, err := EncodeFailedResponse(&rspFrame)
		if err != nil {
			kklog.Errorf("encode failed response: %v", err)
			return nil
		}
		return rspBB
	}

	// 处理 FrameTypeRequest/FrameTypeTell 类型的请求
	respBytes, err := h.OnMsg(context.Background(), fr.P, fr.T)
	if err != nil {
		if fr.T == FrameTypeOneway {
			return nil
		}
		rspFrame.Code = 1
		rspFrame.Err = "远程方法执行失败: " + err.Error()
		rspBB, err := EncodeFailedResponse(&rspFrame)
		if err != nil {
			kklog.Errorf("encode failed response: %v", err)
			return nil
		}
		return rspBB
	}

	if fr.T == FrameTypeOneway {
		return nil
	}

	// encode response
	rspBB, err := EncodeRpcFrameWithPayload(FrameTypeResponse, fr.ID, method, respBytes)
	if err != nil {
		return nil
	}
	//喂给上层函数发送回执
	return rspBB
}

//---------------------------------------------------------------

func newRpcHandler[T any, R any](method string, call RpcHandlerFunc[T, R]) *RpcHandler[T, R] {
	return &RpcHandler[T, R]{
		call:   call,
		method: method,
	}
}

func NewRpcReceiver() *RpcReceiver {
	return &RpcReceiver{
		hdMap: make(map[string]IRpcHandler),
	}
}

func RegistRpcHandler[T any, R any](router *RpcReceiver, method string, call RpcHandlerFunc[T, R]) {
	h := newRpcHandler(method, call)
	router.hdMap[method] = h
}
