package kkrpc

import (
	"context"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kkpool"
)

type ISender interface {
	SendBuffer(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) error
	getPending() *pendingMap
}

type ReqRspInvoker[T any, R any] struct {
	sender ISender
	connId kknet.CONN_ID
}

type OneWayInvoker[T any] struct {
	sender ISender
	connId kknet.CONN_ID
}

// 单向调用器.
// 服务器端调用时，connId为连接ID。
// 客户端调用时，connId为0或任意值，目前未使用。后续如果使用连接池，可以考虑使用池中的某个连接ID，也可以继续任意值，由底层选择真实connId。
func NewOneWayInvoker[T any](sender ISender, connId kknet.CONN_ID) OneWayInvoker[T] {
	return OneWayInvoker[T]{
		sender: sender,
		connId: connId,
	}
}

// 请求响应调用器.
// 服务器端调用时，connId为连接ID。
// 客户端调用时，connId为0或任意值，目前未使用。后续如果使用连接池，可以考虑使用池中的某个连接ID，也可以继续任意值，由底层选择真实connId。
func NewReqRspInvoker[T any, R any](sender ISender, connId kknet.CONN_ID) ReqRspInvoker[T, R] {
	return ReqRspInvoker[T, R]{
		sender: sender,
		connId: connId,
	}
}

// Invoke 同步调用，阻塞直到收到响应或 ctx 取消/超时
func (i ReqRspInvoker[T, R]) Invoke(ctx context.Context, method string, req *T, opts CallConfig, rsp *R) error {
	if i.sender.getPending().IsClosed() {
		return kkerrors.ErrConnClosed
	}
	if !CheckReqResp(req, rsp) {
		kklog.Errorf("req resp type not match")
		return kkerrors.ErrInvalidReqResp
	}
	fixCallConfig(&opts)
	if ctx == nil {
		ctx = context.Background()
	}
	pending := i.sender.getPending()
	reqId := genReqId()
	ch, ok := pending.addCh(reqId)
	if !ok {
		return kkerrors.ErrConnClosed
	}
	defer pending.delCh(reqId)

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

	bb, err := EncodeRpcFrame(FrameTypeRequest, reqId, method, req, deadlineMs)
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	if err = i.sender.SendBuffer(i.connId, bb); err != nil {
		return err
	}

	var timer *time.Timer
	if timeout > 0 {
		timerPool := kkpool.GetGlobalTimerPool()
		timer = timerPool.Get(timeout)
		defer timerPool.Put(timer)
	}

	doReturn := func(fr Frame) error {
		if fr.T != FrameTypeResponse {
			return kkerrors.ErrInvalidFrameType
		}
		err := ErrRpc(fr.Code, fr.Err)
		if err != nil {
			return err
		}
		err = payloadCodec.Unmarshal(fr.P, rsp)
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
				return kkerrors.ErrConnClosed
			}
			return doReturn(fr)
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return kkerrors.ErrTimeout
		}
	} else {
		select {
		case fr, ok := <-ch:
			if !ok {
				return kkerrors.ErrConnClosed
			}
			return doReturn(fr)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// 异步调用（非阻塞等待结果）。若 opts 或 ctx 设置了超时，超时未收到响应会调用 callback(nil, ErrTimeout)，且仅回调一次。
func (i ReqRspInvoker[T, R]) InvokeAsync(ctx context.Context, method string, req *T, opts CallConfig, callback func(rsp *R, err error)) error {
	if i.sender.getPending().IsClosed() {
		return kkerrors.ErrConnClosed
	}

	var respInfo *R = new(R)

	if !CheckReqResp(req, respInfo) {
		kklog.Errorf("req resp type not match")
		return kkerrors.ErrInvalidReqResp
	}

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
	bb, err := EncodeRpcFrame(FrameTypeRequest, reqId, method, req, deadlineMs)
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
		err = payloadCodec.Unmarshal(fr.P, respInfo)
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
		timerPool := kkpool.GetGlobalTimerPool()
		t := timerPool.Get(timeout)
		go func() {
			select {
			case <-t.C:
				if _, ok := pending.takeCallback(reqId); ok {
					callback(nil, kkerrors.ErrTimeout)
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

// 无响应调用（没有结果，单向调用）
func (i OneWayInvoker[T]) InvokeNR(ctx context.Context, method string, req *T, opts CallConfig) error {
	if i.sender.getPending().IsClosed() {
		return kkerrors.ErrConnClosed
	}
	if !CheckOneWay(req) {
		kklog.Errorf("req type not match")
		return kkerrors.ErrInvalidReqResp
	}
	bb, err := EncodeRpcFrame(FrameTypeOneway, 0, method, req, ctxDeadlineUnixMs(ctx))
	if err != nil {
		kklog.Errorf("encode rpc frame: %v", err)
		return err
	}
	err = i.sender.SendBuffer(i.connId, bb)
	if err != nil {
		return err
	}
	return nil
}
