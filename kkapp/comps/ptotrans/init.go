package ptotrans

import (
	"github.com/vvisun/kkdg/remotes/kkrpc"
)

func init() {
	kkrpc.RegisterOneWayMethod[RpcMsgRegister]("register")
	kkrpc.RegisterOneWayMethod[RpcS2Client]("s2c")
	kkrpc.RegisterOneWayMethod[RpcS2Clients]("s2cs")
	kkrpc.RegisterOneWayMethod[RpcC2S]("c2s")
}
