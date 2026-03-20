package hubproto

import (
	"sync"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

var HubMessagePacket = kkpacket.NewFullPacket(
	kkpacket.DefaultStreamPacket(),
	kkpacket.NewMessagePacket(
		kkpacket.NewPacketHeadWithNames(
			[]kkpacket.IHeadPart{
				&kkpacket.PartUint32{},
			},
			[]string{
				kkpacket.PartNameMsgID, // 消息ID
			},
		),
		kkcodec.GetCodec(kkcodec.CodecTypeJson),
		kkpacket.NewMsgRouter(),
	),
)

var initMsgsOnce sync.Once

func InitMsgs() {
	initMsgsOnce.Do(func() {
		router := HubMessagePacket.GetMessageTool().GetRouter()
		router.Register(MsgIDAuthReq, &AuthReq{}, "hub")
		router.Register(MsgIDAuthResp, &AuthResp{}, "hub")
		router.Register(MsgIDRegisterActorReq, &RegisterActorReq{}, "hub")
		router.Register(MsgIDRegisterActorResp, &RegisterActorResp{}, "hub")
		router.Register(MsgIDFindActorReq, &FindActorReq{}, "hub")
		router.Register(MsgIDFindActorResp, &FindActorResp{}, "hub")
		router.Register(MsgIDGetAllActorsOfNodeReq, &GetAllActorsOfNodeReq{}, "hub")
		router.Register(MsgIDGetAllActorsOfNodeResp, &GetAllActorsOfNodeResp{}, "hub")
	})
}
