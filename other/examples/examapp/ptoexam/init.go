package ptoexam

import "github.com/vvisun/kkdg/kknet/kkpacket"

func InitMsgs(router *kkpacket.MsgRouter) {
	router.Register(1, &Msg1Req{}, "logic")
	router.Register(2, &Msg1Resp{}, "logic")
	router.Register(3, &Msg2Broadcast{}, "logic")
}
