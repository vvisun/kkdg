package kkcluster

import (
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

var (
	gMsgCodec      kkcodec.ICodec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	initedMsgCodec atomic.Bool
)

func GetMsgCodec() kkcodec.ICodec {
	return gMsgCodec
}

// 配置默认值。启动阶段初始化，运行期间不要修改。
// @param codec 消息编码器
func ConfigDefaults(codec kkcodec.ICodec) {
	if !initedMsgCodec.CompareAndSwap(false, true) {
		kklog.Warnf("[kkcluster] msg codec already setted, ignore")
		return
	}
	if codec == nil {
		kklog.Errorf("[kkcluster] ConfigDefaults codec is nil, use default codec: %s", "msgpack")
		codec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
	gMsgCodec = codec
}
