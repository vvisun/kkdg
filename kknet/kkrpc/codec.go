package kkrpc

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func EncodeRpcFrame[T any](ft FrameType, reqId uint64, method string, data *T) (*kkbuffer.ByteBuffer, error) {
	switch ft {
	case FrameTypeRequest, FrameTypeResponse:
		if reqId == 0 {
			return nil, ErrInvalidRequestID
		}
	case FrameTypeTell:
		reqId = 0
	}

	// encode args
	argBytes, err := dataCodec.Marshal(data)
	if err != nil {
		return nil, err
	}

	// encode frame
	request := Frame{
		T:  ft,
		ID: reqId,
		M:  method,
		P:  argBytes,
	}
	frameBytes, err := rpcCodec.Marshal(&request)
	if err != nil {
		return nil, err
	}

	// encode stream
	bb, err := kkpacket.DefaultStreamPacket().Pack(frameBytes)
	if err != nil {
		return nil, err
	}
	return bb, nil
}

func DecodeRpcFrame[T any](bb *kkbuffer.ByteBuffer) (*T, *Frame, error) {
	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(bb.Bytes())
	if err != nil {
		return nil, nil, err
	}
	var frame Frame
	if err := rpcCodec.Unmarshal(msgBytes, &frame); err != nil {
		return nil, nil, err
	}
	var data T
	if err := dataCodec.Unmarshal(frame.P, &data); err != nil {
		return nil, nil, err
	}
	return &data, &frame, nil
}
