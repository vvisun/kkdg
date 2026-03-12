package kkrpc

import "github.com/vvisun/kkdg/utils/kkcodec"

type testReq struct {
	ID   int
	Data string
}

type testRsp struct {
	Code int
	Msg  string
}

var (
	// rpc用的编码器
	gFrameCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeFlatBuffer)
	// rpc消息里的Data字段编码器
	gPayloadCodec kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
)
