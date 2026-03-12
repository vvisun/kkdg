package kkrpc

import (
	"context"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kktime"
)

type ReqRspInvoker[T any, R any] struct {
	sender ISender
	connId kknet.CONN_ID
	method string // 构造时验证并缓存，调用时不再 CheckReqResp
}

// NewReqRspInvoker 创建请求响应调用器。method 必须在 RegisterReqRspMethod 中已注册，否则 panic。
// 服务器端调用时 connId 为连接ID；客户端调用时 connId 为 0。
func NewReqRspInvoker[T any, R any](sender ISender, connId kknet.CONN_ID) (ReqRspInvoker[T, R], error) {
	method, ok := verifyReqRespMethod[T, R]()
	if !ok {
		kklog.Errorf("ReqRspInvoker: method %q not registered for types %T, %T", method, (*T)(nil), (*R)(nil))
		return ReqRspInvoker[T, R]{}, kkerrors.ErrRpcMethodNotRegistered
	}
	return ReqRspInvoker[T, R]{
		sender: sender,
		connId: connId,
		method: method,
	}, nil
}

// Invoke 同步调用，阻塞直到收到响应或 ctx 取消/超时
func (i ReqRspInvoker[T, R]) Invoke(ctx context.Context, req *T, opts CallConfig, rsp *R) error {
	if i.sender.getPending().IsClosed() {
		return kkerrors.ErrRpcConnClosed
	}
	fixCallConfig(&opts)
	if ctx == nil {
		ctx = context.Background()
	}
	pending := i.sender.getPending()
	reqId := genReqId()
	ch, ok := pending.addCh(reqId)
	if !ok {
		return kkerrors.ErrRpcConnClosed
	}
	defer func() {
		removed := pending.delCh(reqId)
		if removed == nil && ch != nil {
			// deliver 已移除，channel 在本地，需归还池；若 timeout 与 response 竞态，可能 channel 内有值，先排空
			select {
			case <-ch:
			default:
			}
			pending.putChBack(ch)
		}
	}()

	timeout := opts.Timeout
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d > 0 && (timeout <= 0 || d < timeout) {
			timeout = d
		}
	}
	var deadlineMs int64
	if timeout > 0 {
		deadlineMs = time.Now().Add(timeout).UnixMilli()
	}

	bb, err := EncodeRpcFrame(i.sender.getFrameCodec(), i.sender.getPayloadCodec(), FrameTypeRequest, reqId, i.method, req, deadlineMs)
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	if err = i.sender.SendBuffer(i.connId, bb); err != nil {
		return err
	}

	var timer *time.Timer
	if timeout > 0 {
		timerPool := kktime.GetGlobalTimerPool()
		timer = timerPool.Get(timeout)
		defer timerPool.Put(timer)
	}

	doReturn := func(fr Frame) error {
		if fr.T != FrameTypeResponse {
			return kkerrors.ErrRpcInvalidFrameType
		}
		err := ErrRpc(fr.Code, fr.Err)
		if err != nil {
			return err
		}
		err = i.sender.getPayloadCodec().Unmarshal(fr.P, rsp)
		byteslice.Put(fr.P)
		if err != nil {
			return err
		}
		return nil
	}

	if timer != nil {
		select {
		case fr, ok := <-ch:
			if !ok {
				return kkerrors.ErrRpcConnClosed
			}
			return doReturn(fr)
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return kkerrors.ErrRpcTimeout
		}
	} else {
		select {
		case fr, ok := <-ch:
			if !ok {
				return kkerrors.ErrRpcConnClosed
			}
			return doReturn(fr)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// InvokeAsync 异步调用（非阻塞等待结果）。若 opts 或 ctx 设置了超时，超时未收到响应会调用 callback(nil, ErrTimeout)，且仅回调一次。
func (i ReqRspInvoker[T, R]) InvokeAsync(ctx context.Context, req *T, opts CallConfig, callback func(rsp *R, err error)) error {
	if i.sender.getPending().IsClosed() {
		return kkerrors.ErrRpcConnClosed
	}
	var respInfo *R = new(R)
	fixCallConfig(&opts)
	if ctx == nil {
		ctx = context.Background()
	}

	timeout := opts.Timeout
	if dl, ok := ctx.Deadline(); ok {
		if d := time.Until(dl); d > 0 && (timeout <= 0 || d < timeout) {
			timeout = d
		}
	}
	var deadlineMs int64
	if timeout > 0 {
		deadlineMs = time.Now().Add(timeout).UnixMilli()
	}

	reqId := genReqId()
	bb, err := EncodeRpcFrame(i.sender.getFrameCodec(), i.sender.getPayloadCodec(), FrameTypeRequest, reqId, i.method, req, deadlineMs)
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	pending := i.sender.getPending()

	var doneCh chan struct{}
	if timeout > 0 {
		doneCh = make(chan struct{})
	}

	pending.addCallback(reqId, func(fr Frame) {
		if fr.ID != reqId || fr.T != FrameTypeResponse {
			return
		}
		pending.delCallback(reqId)
		if doneCh != nil {
			close(doneCh)
		}

		err := ErrRpc(fr.Code, fr.Err)
		if err != nil {
			callback(nil, err)
			return
		}
		err = i.sender.getPayloadCodec().Unmarshal(fr.P, respInfo)
		byteslice.Put(fr.P)
		if err != nil {
			callback(nil, err)
			return
		}
		callback(respInfo, nil)
	})
	err = i.sender.SendBuffer(i.connId, bb)
	if err != nil {
		callback(nil, err)
		pending.delCallback(reqId)
		return err
	}

	if timeout > 0 {
		timerPool := kktime.GetGlobalTimerPool()
		t := timerPool.Get(timeout)
		go func() {
			select {
			case <-t.C:
				if _, ok := pending.takeCallback(reqId); ok {
					callback(nil, kkerrors.ErrRpcTimeout)
				}
			case <-doneCh:
				if !t.Stop() {
					select {
					case <-t.C:
					default:
					}
				}
			}
			timerPool.Put(t)
		}()
	}
	return nil
}
