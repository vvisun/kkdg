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
// @param codec 消息编码器
func ConfigDefaults(codec kkcodec.ICodec, registry *MessageRegistry) {
	if codec != nil {
		msgCodec = codec
	}
	if registry != nil {
		defaultMessageRegistry = registry
	}
}
