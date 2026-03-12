package envconfig

import (
	"encoding/binary"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type EnvConfig struct {
	// 默认的字节序
	ByteOrderDefault binary.ByteOrder
	// 默认的流拆解器
	StreamToolDefault kkpacket.IPacket
	// 网关与客户端之间的消息编码解码器
	PacketGateAndClient *kkpacket.MessagePacket
	// 网关与业务服之间的消息编码解码器
	PacketGateAndBusiness *kkpacket.MessagePacket
	// 远程Actor消息编码解码器
	MsgCodecActor kkcodec.ICodec
	// 远程Actor消息注册表
	MessageRegistryActor *actorremotes.MessageRegistry
}

// 配置默认值。启动阶段初始化，运行期间不要修改。
// @param cfg 环境配置
func ConfigDefaults(cfg *EnvConfig) {
	if cfg == nil {
		kklog.Warn("[envconfig] config is nil, ignore")
		return
	}
	kkapp.ConfigDefaults(cfg.StreamToolDefault, cfg.PacketGateAndClient, cfg.PacketGateAndBusiness)
	actorremotes.ConfigDefaults(cfg.MsgCodecActor, cfg.MessageRegistryActor)
	kkpacket.ConfigDefaults(cfg.ByteOrderDefault)
}
