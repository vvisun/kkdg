package kkrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type ReqRspHandlerFunc[T any, R any] func(ctx context.Context, msg *T, resp *R, connId kknet.CONN_ID) error
type OneWayHandlerFunc[T any] func(ctx context.Context, msg *T, connId kknet.CONN_ID) error

// 消息接收器 req resp
type IReqRspHandler interface {
	// GetMethod 返回 RPC 方法名
	GetMethod() string
	// 消息回调
	OnMsg(ctx context.Context, payload []byte, frameType FrameType, connId kknet.CONN_ID) ([]byte, error)
}

// 消息接收器 oneway
type IOneWayHandler interface {
	// GetMethod 返回 RPC 方法名
	GetMethod() string
	// 消息回调
	OnMsg(ctx context.Context, payload []byte, frameType FrameType, connId kknet.CONN_ID) error
}

//----------------------------------------------------------------

type OneWayHandler[T any] struct {
	call         OneWayHandlerFunc[T]
	method       string
	payloadCodec kkcodec.ICodec
}

func (h *OneWayHandler[T]) GetMethod() string {
	return h.method
}

func (h *OneWayHandler[T]) OnMsg(ctx context.Context, payload []byte, frameType FrameType, connId kknet.CONN_ID) error {
	var data T
	if err := h.payloadCodec.Unmarshal(payload, &data); err != nil {
		return err
	}
	err := h.call(ctx, &data, connId)
	if err != nil {
		return err
	}
	return nil
}

//----------------------------------------------------------------

type ReqRspHandler[T any, R any] struct {
	call         ReqRspHandlerFunc[T, R]
	method       string
	payloadCodec kkcodec.ICodec
}

func (h *ReqRspHandler[T, R]) GetMethod() string {
	return h.method
}

func (h *ReqRspHandler[T, R]) OnMsg(ctx context.Context, payload []byte, frameType FrameType, connId kknet.CONN_ID) ([]byte, error) {
	var data T
	var resp R
	if err := h.payloadCodec.Unmarshal(payload, &data); err != nil {
		return nil, err
	}
	// kklog.Debugf("收到远程方法调用请求: %v", data)
	err := h.call(ctx, &data, &resp, connId)
	if err != nil {
		return nil, err
	}

	if frameType == FrameTypeOneway {
		return nil, nil
	}

	respBytes, err := h.payloadCodec.Marshal(&resp)
	if err != nil {
		return nil, err
	}
	return respBytes, nil
}

//---------------------------------------------------------------

type RpcReceiver struct {
	stats     *RpcStats
	hdMap     map[string]IReqRspHandler
	oneWayMap map[string]IOneWayHandler
	rpcOpts   RpcOption
}

func (r *RpcReceiver) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer, pending *pendingMap) *kkbuffer.ByteBuffer {
	var stats *RpcStats
	if pending != nil {
		stats = pending.stats
	}
	frameBytes, err := r.rpcOpts.StreamTool.Unpack(data.Bytes())
	if err != nil {
		if stats != nil {
			stats.AddInternalError()
		}
		kkbuffer.Put(data)
		return nil
	}
	var fr Frame
	err = r.rpcOpts.FrameCodec.Unmarshal(frameBytes, &fr)
	if err != nil {
		kklog.Debugf("failed to unmarshal frame: %v", err)
		if stats != nil {
			stats.AddInternalError()
		}
		kkbuffer.Put(data)
		return nil
	}

	switch fr.T {
	case FrameTypeOneway:
		r.dealOneWay(&fr, connId)
		kkbuffer.Put(data)
		return nil
	case FrameTypeRequest:
		bb := r.dealReqResp(&fr, connId)
		kkbuffer.Put(data)
		return bb
	case FrameTypeResponse:
		// fr.P 指向 data.B 内部，必须在 Put 前拷贝，否则 buffer 被池复用会覆盖 payload（并发时必现）
		if fr.P != nil {
			pLen := len(fr.P)
			if pLen > 0 {
				oldP := fr.P
				fr.P = byteslice.GetWithLenCap(pLen, pLen)
				copy(fr.P, oldP)
			}
		}
		kkbuffer.Put(data)
		// 收到了远程方法的返回结果（同步 Invoke 或异步 callback）
		if pending != nil {
			pending.deliver(fr.ID, fr)
		}
		return nil
	default:
		if stats != nil {
			stats.AddInternalError()
		}
		kkbuffer.Put(data)
		kklog.Debugf("收到未知类型的消息: %v, %v", fr.T, fr.M)
		return nil
	}
}

func (r *RpcReceiver) dealReqResp(fr *Frame, connId kknet.CONN_ID) *kkbuffer.ByteBuffer {
	// 处理 FrameTypeRequest 类型的请求
	rspFrame := Frame{
		T:    FrameTypeResponse,
		ID:   fr.ID,
		M:    fr.M,
		Code: ErrorCodeSuccess,
		Err:  "",
	}

	method := fr.M
	h, ok := r.hdMap[method]
	if !ok || h == nil {
		rspFrame.Code = ErrorCodeMethodNotFound
		rspFrame.Err = "未找到远程方法" + method
		rspBB, err := EncodeFailedResponse(r.rpcOpts.StreamTool, r.rpcOpts.FrameCodec, &rspFrame)
		if err != nil {
			kklog.Errorf("encode failed response: %v", err)
			return nil
		}
		return rspBB
	}

	ctx, cancel := deadlineCtx(fr.DL)
	defer cancel()
	respBytes, err := h.OnMsg(ctx, fr.P, fr.T, connId)
	if err != nil {
		rspFrame.Code = ErrorCodeMethodRetErr
		rspFrame.Err = "远程方法执行失败: " + err.Error()
		rspBB, err := EncodeFailedResponse(r.rpcOpts.StreamTool, r.rpcOpts.FrameCodec, &rspFrame)
		if err != nil {
			kklog.Errorf("encode failed response: %v", err)
			return nil
		}
		return rspBB
	}

	// encode response
	rspBB, err := EncodeRpcFrameWithPayload(r.rpcOpts.StreamTool, r.rpcOpts.FrameCodec, r.rpcOpts.PayloadCodec, FrameTypeResponse, fr.ID, method, respBytes, 0)
	if err != nil {
		return nil
	}
	//喂给上层函数发送回执
	return rspBB
}

func (r *RpcReceiver) dealOneWay(fr *Frame, connId kknet.CONN_ID) error {
	method := fr.M
	h, ok := r.oneWayMap[method]
	if !ok || h == nil {
		return nil
	}
	ctx, cancel := deadlineCtx(fr.DL)
	defer cancel()
	return h.OnMsg(ctx, fr.P, fr.T, connId)
}

func NewRpcReceiver(rpcOpts RpcOption) *RpcReceiver {
	CheckRpcOption(&rpcOpts)
	return &RpcReceiver{
		hdMap:     make(map[string]IReqRspHandler),
		oneWayMap: make(map[string]IOneWayHandler),
		rpcOpts:   rpcOpts,
	}
}
