package kkrpc

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func EncodeFailedResponse(methodMgr *MethodManager, frame *Frame) (*kkbuffer.ByteBuffer, error) {
	if frame.Code == ErrorCodeSuccess {
		frame.Code = ErrorCodeFailed
	}
	if frame.Err == "" {
		frame.Err = "unknown error"
	}

	lfbCount := methodMgr.streamTool.LengthFieldByteCount()
	bb1, err1 := methodMgr.frameCodec.MarshalAppend(frame, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	methodMgr.streamTool.WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func EncodeRpcFrameWithPayload(
	methodMgr *MethodManager,
	ft FrameType,
	reqId uint64,
	method string,
	payload []byte,
	deadlineMs int64,
) (*kkbuffer.ByteBuffer, error) {
	switch ft {
	case FrameTypeRequest, FrameTypeResponse:
		if reqId == 0 {
			return nil, kkerrors.ErrRpcInvalidRequestID
		}
	case FrameTypeOneway:
		reqId = 0
	}

	// encode frame
	request := Frame{
		T:  ft,
		ID: reqId,
		DL: deadlineMs,
		M:  method,
		P:  payload,
	}

	lfbCount := methodMgr.streamTool.LengthFieldByteCount()
	bb1, err1 := methodMgr.frameCodec.MarshalAppend(&request, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	methodMgr.streamTool.WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func EncodeRpcFrame(
	methodMgr *MethodManager,
	ft FrameType,
	reqId uint64,
	method string,
	msg any,
	deadlineMs int64,
) (*kkbuffer.ByteBuffer, error) {
	payloadBytes, err := methodMgr.payloadCodec.Marshal(msg)
	if err != nil {
		return nil, err
	}

	switch ft {
	case FrameTypeRequest, FrameTypeResponse:
		if reqId == 0 {
			return nil, kkerrors.ErrRpcInvalidRequestID
		}
	case FrameTypeOneway:
		reqId = 0
	}

	// encode frame
	request := Frame{
		T:  ft,
		ID: reqId,
		DL: deadlineMs,
		M:  method,
		P:  payloadBytes,
	}

	lfbCount := methodMgr.streamTool.LengthFieldByteCount()
	bb1, err1 := methodMgr.frameCodec.MarshalAppend(&request, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	methodMgr.streamTool.WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func DecodeRpcPayload[T any](methodMgr *MethodManager, bb *kkbuffer.ByteBuffer) (*T, error) {
	frameBytes, err := methodMgr.streamTool.Unpack(bb.Bytes())
	if err != nil {
		return nil, err
	}

	var frame Frame
	err = methodMgr.frameCodec.Unmarshal(frameBytes, &frame)
	if err != nil {
		return nil, err
	}

	payloadBytes := frame.P
	var t T
	err = methodMgr.payloadCodec.Unmarshal(payloadBytes, &t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
