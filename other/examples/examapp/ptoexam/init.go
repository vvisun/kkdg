package ptoexam

import "github.com/vvisun/kkdg/kknet/kkpacket"

func InitMsgs(router *kkpacket.MsgRouter) {
	// test.proto
	router.Register(1, &Msg1Req{}, "logic")
	router.Register(2, &Msg1Resp{}, "logic")
	router.Register(3, &Msg2Broadcast{}, "logic")
	// test1.proto
	router.Register(1000, &AaaaReq{}, "combat")
	router.Register(1001, &AaaaResp{}, "combat")
	router.Register(1002, &BbbbResp{}, "combat")
}
