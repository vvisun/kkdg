package kkrpc

import (
	"reflect"

	"github.com/vvisun/kkdg/utils/kkcodec"
)

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

// clearRpcManagerForTest 清空 gRpcManager 中所有注册信息。
// 仅用于测试场景，便于多测试重复注册。生产环境请勿调用。
func clearRpcManagerForTest() {
	gRpcManager.mu.Lock()
	defer gRpcManager.mu.Unlock()
	gRpcManager.type2methodReqRsp = make(map[reflect.Type]string)
	gRpcManager.method2typeReqRsp = make(map[string]methodReqRsp)
	gRpcManager.type2methodOneWay = make(map[reflect.Type]string)
	gRpcManager.method2typeOneWay = make(map[string]methonOneWay)
}
