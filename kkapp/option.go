package kkapp

import (
	"github.com/vvisun/kkdg/kkapp/faultreport"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

type AppOptions struct {
	// 配置文件所在目录
	ConfigsDir string
	// 理论上可以分别设置transport和client的stream工具。
	// 但为了简化配置，直接都使用同一个了，影响不大，就是一个[length]字段用2字节还是4字节的问题而已。
	StreamTool kkpacket.IPacket
	// 客户端与服务器之间的协议约定。
	// ClientMsgPacket.router外部应该为他注册消息。否则客户端发来消息时，找不到对应的编解码器。
	ClientMsgPacket *kkpacket.MessagePacket
	// 转发层的消息编解码器。
	TransportorCodec kkcodec.ICodec
	// 故障处理规则表。
	FaultRuleMap map[string]faultreport.EFaultAction
}

func DefaultOptions() AppOptions {
	return AppOptions{
		StreamTool: kkpacket.DefaultStreamPacket(),
		ClientMsgPacket: kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeJson),
			kkpacket.NewMsgRouter(),
		),
		TransportorCodec: kkcodec.GetCodec(kkcodec.CodecTypeJson),
		FaultRuleMap:     make(map[string]faultreport.EFaultAction),
	}
}

func CheckOptions(opt *AppOptions) {
	if opt == nil {
		kklog.PanicLog("[kkapp] options is nil, must be set")
		return
	}
	if opt.StreamTool == nil {
		kklog.Warnf("[kkapp] stream tool is nil, use default stream tool")
		opt.StreamTool = kkpacket.DefaultStreamPacket()
	}
	if opt.ClientMsgPacket == nil {
		kklog.Warnf("[kkapp] client msg packet is nil, use default client msg packet")
		opt.ClientMsgPacket = kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeJson),
			kkpacket.NewMsgRouter(),
		)
	}
	if opt.TransportorCodec == nil {
		kklog.Warnf("[kkapp] transportor codec is nil, use default codec: %s", "json")
		opt.TransportorCodec = kkcodec.GetCodec(kkcodec.CodecTypeJson)
	}

	if opt.FaultRuleMap == nil {
		kklog.Warnf("[kkapp] fault rule map is nil, use default map, will stop app when any component fault")
		opt.FaultRuleMap = make(map[string]faultreport.EFaultAction)
	} else {
		for compName, action := range opt.FaultRuleMap {
			if compName == "" {
				kklog.PanicLog("[kkapp] fault compName is empty")
			}
			if !faultreport.IsValidFaultAction(action) {
				kklog.PanicLog("[kkapp] fault compName %s action %d is invalid", compName, action)
			}
		}
	}
}

func ApplyOptions(opts ...func(o *AppOptions)) AppOptions {
	cfg := DefaultOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	CheckOptions(&cfg)
	return cfg
}

func WithStreamTool(streamTool kkpacket.IPacket) func(o *AppOptions) {
	return func(o *AppOptions) {
		if streamTool == nil {
			return
		}
		o.StreamTool = streamTool
	}
}

func WithClientMsgPacket(clientMsgPacket *kkpacket.MessagePacket) func(o *AppOptions) {
	return func(o *AppOptions) {
		if clientMsgPacket == nil {
			return
		}
		o.ClientMsgPacket = clientMsgPacket
	}
}

func WithTransportorCodec(codec kkcodec.ICodec) func(o *AppOptions) {
	return func(o *AppOptions) {
		o.TransportorCodec = codec
	}
}

func WithFaultAction(compName string, action faultreport.EFaultAction) func(o *AppOptions) {
	return func(o *AppOptions) {
		if compName == "" {
			kklog.Errorf("[kkapp] fault action compName is empty")
			return
		}
		if !faultreport.IsValidFaultAction(action) {
			kklog.Errorf("[kkapp] fault compName %s action %d is invalid", compName, action)
			return
		}
		if o.FaultRuleMap == nil {
			o.FaultRuleMap = make(map[string]faultreport.EFaultAction)
		}
		o.FaultRuleMap[compName] = action
	}
}

func WithFaultActionMap(faultActionMap map[string]faultreport.EFaultAction) func(o *AppOptions) {
	return func(o *AppOptions) {
		if faultActionMap == nil {
			return
		}
		if o.FaultRuleMap == nil {
			o.FaultRuleMap = make(map[string]faultreport.EFaultAction)
		}
		for compName, action := range faultActionMap {
			if !faultreport.IsValidFaultAction(action) {
				kklog.Errorf("[kkapp] fault compName %s action %d is invalid", compName, action)
				continue
			}
			o.FaultRuleMap[compName] = action
		}
	}
}

// WithCoreComponent 是默认配置核心组件的故障处理动作。
// 核心组件是指：如果该组件故障，则停止应用。
func WithCoreComponent(compName string) func(o *AppOptions) {
	return WithFaultAction(compName, faultreport.FaultActionStopApp)
}

// WithNotCoreComponent 是默认配置非核心组件的故障处理动作。
// 非核心组件是指：如果该组件故障，则停止组件。
func WithNotCoreComponent(compName string) func(o *AppOptions) {
	return WithFaultAction(compName, faultreport.FaultActionStopComp)
}

func WithConfigDir(configsDir string) func(o *AppOptions) {
	return func(o *AppOptions) {
		o.ConfigsDir = configsDir
	}
}
