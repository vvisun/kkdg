package kkrpc

import "github.com/vvisun/kkdg/proto/pbrpc"

type FrameType = uint32

const (
	FrameTypeUnknown       FrameType = 0
	FrameTypeRequest       FrameType = 1
	FrameTypeResponse      FrameType = 2
	FrameTypeOneway        FrameType = 3
	FrameTypeTransMsg      FrameType = 4
	FrameTypeTransBroadMsg FrameType = 5
)

type Frame = pbrpc.Frame

type (
	// TransMsg 网关转发消息
	TransMsg = pbrpc.TransMsg

	// TransBroadcast 网关转发群发消息
	TransBroadMsg = pbrpc.TransBroadMsg
)
