package kkrpc

import (
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

func EncodeRpcFrame(ft FrameType, reqId uint64, method string, argBytes []byte) (*kkbuffer.ByteBuffer, error) {
	switch ft {
	case FrameTypeRequest, FrameTypeResponse:
		if reqId == 0 {
			return nil, ErrInvalidRequestID
		}
	case FrameTypeTell:
		reqId = 0
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
		kkbuffer.Put(bb)
		return nil, err
	}
	return bb, nil
}

func EncodeRpcFrameEx(ft FrameType, reqId uint64, method string, msg any) (*kkbuffer.ByteBuffer, error) {
	argBytes, err := dataCodec.Marshal(msg)
	if err != nil {
		return nil, err
	}

	switch ft {
	case FrameTypeRequest, FrameTypeResponse:
		if reqId == 0 {
			return nil, ErrInvalidRequestID
		}
	case FrameTypeTell:
		reqId = 0
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
		kkbuffer.Put(bb)
		return nil, err
	}
	return bb, nil
}
