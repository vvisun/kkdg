package kkrpc

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

func EncodeFailedResponse(
	streamTool kkpacket.IPacket,
	frameCodec kkcodec.ICodec,
	frame *Frame,
) (*kkbuffer.ByteBuffer, error) {
	if frame.Code == ErrorCodeSuccess {
		frame.Code = ErrorCodeFailed
	}
	if frame.Err == "" {
		frame.Err = "unknown error"
	}

	lfbCount := streamTool.LengthFieldByteCount()
	bb1, err1 := frameCodec.MarshalAppend(frame, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	streamTool.WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func EncodeRpcFrameWithPayload(
	streamTool kkpacket.IPacket,
	frameCodec kkcodec.ICodec,
	payloadCodec kkcodec.ICodec,
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

	lfbCount := streamTool.LengthFieldByteCount()
	bb1, err1 := frameCodec.MarshalAppend(&request, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	streamTool.WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func EncodeRpcFrameEx(
	streamTool kkpacket.IPacket,
	frameCodec kkcodec.ICodec,
	payloadCodec kkcodec.ICodec,
	ft FrameType,
	reqId uint64,
	msg any,
	deadlineMs int64,
) (*kkbuffer.ByteBuffer, error) {
	method := gRpcManager.getMethod(msg)
	if method == "" {
		return nil, kkerrors.ErrRpcMethodNotRegistered
	}
	return EncodeRpcFrame(streamTool, frameCodec, payloadCodec, ft, reqId, method, msg, deadlineMs)
}

func EncodeRpcFrame(
	streamTool kkpacket.IPacket,
	frameCodec kkcodec.ICodec,
	payloadCodec kkcodec.ICodec,
	ft FrameType,
	reqId uint64,
	method string,
	msg any,
	deadlineMs int64,
) (*kkbuffer.ByteBuffer, error) {
	payloadBytes, err := payloadCodec.Marshal(msg)
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

	lfbCount := streamTool.LengthFieldByteCount()
	bb1, err1 := frameCodec.MarshalAppend(&request, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	streamTool.WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func DecodeRpcPayload[T any](
	streamTool kkpacket.IPacket,
	frameCodec kkcodec.ICodec,
	payloadCodec kkcodec.ICodec,
	bb *kkbuffer.ByteBuffer,
) (*T, error) {
	frameBytes, err := streamTool.Unpack(bb.Bytes())
	if err != nil {
		return nil, err
	}

	var frame Frame
	err = frameCodec.Unmarshal(frameBytes, &frame)
	if err != nil {
		return nil, err
	}

	payloadBytes := frame.P
	var t T
	err = payloadCodec.Unmarshal(payloadBytes, &t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
