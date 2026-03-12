// Package kkapp 提供应用程序框架。
//
// 注意：在app初始化阶段，调用ConfigDefaults()配置默认值。运行期间不要修改。
// eg:
//
//	envconfig.ConfigDefaults(&envconfig.EnvConfig{
//		ByteOrderDefault: binary.BigEndian,
//		StreamToolDefault: kkpacket.DefaultStreamPacket(),
//		PacketGateAndClient: kkpacket.NewMessagePacket(
//			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
//			kkcodec.GetCodec(kkcodec.CodecTypeJson),
//			kkpacket.NewMsgRouter(),
//		),
//		PacketGateAndBusiness: kkpacket.NewMessagePacket(
//			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
//			kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
//			kkpacket.NewMsgRouter(),
//		),
//		MsgCodecActor: kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
//		MessageRegistryActor: actorremotes.NewMessageRegistry(),
//	})
package kkapp
