package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type ReqRspHandlerFunc[T any, R any] func(ctx context.Context, msg *T, resp *R) error
type OneWayHandlerFunc[T any] func(ctx context.Context, msg *T) error

// 消息接收器 req resp
type IRpcHandler interface {
	// GetMethod 返回 RPC 方法名
	GetMethod() string
	// 消息回调
	OnMsg(ctx context.Context, payload []byte, frameType FrameType) ([]byte, error)
}

// 消息接收器 oneway
type IOneWayHandler interface {
	// GetMethod 返回 RPC 方法名
	GetMethod() string
	// 消息回调
	OnMsg(ctx context.Context, payload []byte, frameType FrameType) error
}

//----------------------------------------------------------------

type OneWayHandler[T any] struct {
	call   OneWayHandlerFunc[T]
	method string
}

func (h *OneWayHandler[T]) GetMethod() string {
	return h.method
}

func (h *OneWayHandler[T]) OnMsg(ctx context.Context, payload []byte, frameType FrameType) error {
	var data T
	if err := payloadCodec.Unmarshal(payload, &data); err != nil {
		return err
	}
	err := h.call(ctx, &data)
	if err != nil {
		return err
	}
	return nil
}

//----------------------------------------------------------------

type ReqRspHandler[T any, R any] struct {
	call   ReqRspHandlerFunc[T, R]
	method string
}

func (h *ReqRspHandler[T, R]) GetMethod() string {
	return h.method
}

func (h *ReqRspHandler[T, R]) OnMsg(ctx context.Context, payload []byte, frameType FrameType) ([]byte, error) {
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
	hdMap     map[string]IRpcHandler
	oneWayMap map[string]IOneWayHandler
}

func (r *RpcReceiver) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer, pending *pendingMap) *kkbuffer.ByteBuffer {
	frameBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		kkbuffer.Put(data)
		return nil
	}
	var fr Frame
	err = frameCodec.Unmarshal(frameBytes, &fr)
	if err != nil {
		kklog.Debugf("failed to unmarshal frame: %v", err)
		kkbuffer.Put(data)
		return nil
	}
	// fr.P 指向 data.B 内部，必须在 Put 前拷贝，否则 buffer 被池复用会覆盖 payload（并发时必现）
	if fr.P != nil {
		fr.P = append([]byte(nil), fr.P...)
	}
	kkbuffer.Put(data)

	switch fr.T {
	case FrameTypeOneway:
		r.dealOneWay(&fr)
		return nil
	case FrameTypeRequest:
		return r.dealReqResp(&fr)
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
}

func (r *RpcReceiver) dealReqResp(fr *Frame) *kkbuffer.ByteBuffer {
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

	// 处理 FrameTypeRequest 类型的请求
	respBytes, err := h.OnMsg(context.Background(), fr.P, fr.T)
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
	rspBB, err := EncodeRpcFrameWithPayload(FrameTypeResponse, fr.ID, method, respBytes)
	if err != nil {
		return nil
	}
	//喂给上层函数发送回执
	return rspBB
}

func (r *RpcReceiver) dealOneWay(fr *Frame) error {
	method := fr.M
	h, ok := r.oneWayMap[method]
	if !ok || h == nil {
		return nil
	}
	return h.OnMsg(context.Background(), fr.P, fr.T)
}
