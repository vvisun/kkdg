package kkrpc

import (
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

var (
	// rpc用的编码器
	frameCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeFlatBuffer)
	// rpc消息里的Data字段编码器
	payloadCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	// 网关消息使用的编码器. 需和客户端约定好编码器类型
	// gatewayCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeProtoBuf)
)

// 注意，msgpack有并发安全问题，不要使用
func SetFrameCodec(codec kkcodec.ICodec) {
	if codec == nil {
		panic("codec is nil")
	}
	frameCodec = codec
}

// 注意，msgpack有并发安全问题，不要使用
func SetPayloadCodec(codec kkcodec.ICodec) {
	if codec == nil {
		panic("codec is nil")
	}
	payloadCodec = codec
}

func EncodeFailedResponse(frame *Frame) (*kkbuffer.ByteBuffer, error) {
	if frame.Code == ErrorCodeSuccess {
		frame.Code = ErrorCodeFailed
	}
	if frame.Err == "" {
		frame.Err = "unknown error"
	}

	lfbCount := kkpacket.DefaultStreamPacket().LengthFieldByteCount()
	bb1, err1 := frameCodec.MarshalAppend(frame, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	kkpacket.DefaultStreamPacket().WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func EncodeRpcFrameWithPayload(ft FrameType, reqId uint64, method string, payload []byte, deadlineMs int64) (*kkbuffer.ByteBuffer, error) {
	switch ft {
	case FrameTypeRequest, FrameTypeResponse:
		if reqId == 0 {
			return nil, kkerrors.ErrInvalidRequestID
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

	lfbCount := kkpacket.DefaultStreamPacket().LengthFieldByteCount()
	bb1, err1 := frameCodec.MarshalAppend(&request, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	kkpacket.DefaultStreamPacket().WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func EncodeRpcFrame[T any](ft FrameType, reqId uint64, method string, msg *T, deadlineMs int64) (*kkbuffer.ByteBuffer, error) {
	payloadBytes, err := payloadCodec.Marshal(msg)
	if err != nil {
		return nil, err
	}

	switch ft {
	case FrameTypeRequest, FrameTypeResponse:
		if reqId == 0 {
			return nil, kkerrors.ErrInvalidRequestID
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

	lfbCount := kkpacket.DefaultStreamPacket().LengthFieldByteCount()
	bb1, err1 := frameCodec.MarshalAppend(&request, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	kkpacket.DefaultStreamPacket().WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func DecodeRpcPayload[T any](bb *kkbuffer.ByteBuffer) (*T, error) {
	frameBytes, err := kkpacket.DefaultStreamPacket().Unpack(bb.Bytes())
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
