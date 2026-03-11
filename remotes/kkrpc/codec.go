package kkrpc

import (
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

// 提供为全局变量，启动阶段初始化，运行期间不要修改。
// 为了减少多余的心力花在对齐 rpc client和server 的流拆解器，导致编码解码不一致。
var (
	initedCodec atomic.Bool
	// rpc用的编码器
	gFrameCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeFlatBuffer)
	// rpc消息里的Data字段编码器
	gPayloadCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	// 默认的流拆解器
	gStreamTool kkpacket.IPacket = kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
)

// 配置默认值。启动阶段初始化，运行期间不要修改。
// 为了减少多余的心力花在对齐 rpc client和server 的流拆解器，导致编码解码不一致。
//
//	@param frameCodec 帧编码器
//	@param payloadCodec 消息编码器
//	@param streamTool 流拆解器
func ConfigDefaults(frameCodec kkcodec.ICodec, payloadCodec kkcodec.ICodec, streamTool kkpacket.IPacket) {
	if !initedCodec.CompareAndSwap(false, true) {
		kklog.Warnf("[kkrpc] codec already setted, ignore")
		return
	}
	if frameCodec != nil {
		gFrameCodec = frameCodec
	}
	if payloadCodec != nil {
		gPayloadCodec = payloadCodec
	}
	if streamTool != nil {
		gStreamTool = streamTool
	}
}

func EncodeFailedResponse(frame *Frame) (*kkbuffer.ByteBuffer, error) {
	if frame.Code == ErrorCodeSuccess {
		frame.Code = ErrorCodeFailed
	}
	if frame.Err == "" {
		frame.Err = "unknown error"
	}

	lfbCount := gStreamTool.LengthFieldByteCount()
	bb1, err1 := gFrameCodec.MarshalAppend(frame, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	gStreamTool.WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func EncodeRpcFrameWithPayload(ft FrameType, reqId uint64, method string, payload []byte, deadlineMs int64) (*kkbuffer.ByteBuffer, error) {
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

	lfbCount := gStreamTool.LengthFieldByteCount()
	bb1, err1 := gFrameCodec.MarshalAppend(&request, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	gStreamTool.WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func EncodeRpcFrameEx(ft FrameType, reqId uint64, msg any, deadlineMs int64) (*kkbuffer.ByteBuffer, error) {
	method := gRpcManager.getMethod(msg)
	if method == "" {
		return nil, kkerrors.ErrRpcMethodNotRegistered
	}
	return EncodeRpcFrame(ft, reqId, method, msg, deadlineMs)
}

func EncodeRpcFrame(ft FrameType, reqId uint64, method string, msg any, deadlineMs int64) (*kkbuffer.ByteBuffer, error) {
	payloadBytes, err := gPayloadCodec.Marshal(msg)
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

	lfbCount := gStreamTool.LengthFieldByteCount()
	bb1, err1 := gFrameCodec.MarshalAppend(&request, lfbCount)
	if err1 != nil {
		kkbuffer.Put(bb1)
		return nil, err1
	}
	gStreamTool.WriteMessageSize(bb1.B, len(bb1.B)-lfbCount)
	return bb1, nil
}

func DecodeRpcPayload[T any](bb *kkbuffer.ByteBuffer) (*T, error) {
	frameBytes, err := gStreamTool.Unpack(bb.Bytes())
	if err != nil {
		return nil, err
	}

	var frame Frame
	err = gFrameCodec.Unmarshal(frameBytes, &frame)
	if err != nil {
		return nil, err
	}

	payloadBytes := frame.P
	var t T
	err = gPayloadCodec.Unmarshal(payloadBytes, &t)
	if err != nil {
		return nil, err
	}
	return &t, nil
}
