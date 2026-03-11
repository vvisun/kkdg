package actorremotes

import (
	"github.com/vvisun/kkdg/utils/kkcodec"
)

var (
	msgCodec               = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	defaultMessageRegistry = NewMessageRegistry()
)

func GetDefaultMessageRegistry() *MessageRegistry {
	return defaultMessageRegistry
}

// 配置默认值。启动阶段初始化，运行期间不要修改。
// 为了减少多余的心力花在对齐 远程Actor消息编码解码器，导致编码解码不一致。
//
//	@param codec 远程Actor消息编码器
//	@param registry 远程Actor消息注册表
func ConfigDefaults(codec kkcodec.ICodec, registry *MessageRegistry) {
	if codec != nil {
		msgCodec = codec
	}
	if registry != nil {
		defaultMessageRegistry = registry
	}
}
