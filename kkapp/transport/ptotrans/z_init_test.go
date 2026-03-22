package ptotrans

import (
	"reflect"
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkrpc"
)

func TestInitShardMsgs_RegistersAllMsgIDs(t *testing.T) {
	r := kkpacket.NewMsgRouter()
	InitShardMsgs(r)

	cases := []struct {
		id   kkpacket.MSGID
		want reflect.Type
	}{
		{MsgIDRpcMsgRegister, reflect.TypeFor[*RpcMsgRegister]()},
		{MsgIDRpcS2Client, reflect.TypeFor[*RpcS2Client]()},
		{MsgIDRpcS2Clients, reflect.TypeFor[*RpcS2Clients]()},
		{MsgIDRpcC2S, reflect.TypeFor[*RpcC2S]()},
		{MsgIDRpcClientDisconnect, reflect.TypeFor[*RpcClientDisconnect]()},
		{MsgIDRpcAllocClient, reflect.TypeFor[*RpcAllocClient]()},
		{MsgIDRpcClientLoginLogout, reflect.TypeFor[*RpcClientLoginLogout]()},
		{MsgIDRpcUnregister, reflect.TypeFor[*RpcUnregister]()},
	}
	for _, c := range cases {
		tp := r.GetMsgType(c.id)
		if tp == nil {
			t.Fatalf("msg id %d not registered", c.id)
		}
		if tp != c.want {
			t.Fatalf("msg id %d: got type %v want %v", c.id, tp, c.want)
		}
		route, err := r.GetMsgRoute(c.id)
		if err != nil || route != "logic" {
			t.Fatalf("msg id %d route: %q err %v", c.id, route, err)
		}
	}
}

func TestInitRpcMsgs_NoPanic(t *testing.T) {
	mgr := kkrpc.NewMethodManager(nil, nil, nil)
	InitRpcMsgs(mgr)
	// kkrpc 当前对同类型重复 RegisterOneWayMethod 会静默覆盖，此处只保证初始化可重复调用不崩溃
	InitRpcMsgs(mgr)
}

func TestFuncNameConstants(t *testing.T) {
	for _, s := range []string{
		FuncNameRegister,
		FuncNameC2S,
		FuncNameSendToClient,
		FuncNameSendToClients,
		FuncNameClientDisconnect,
		FuncNameAllocClient,
		FuncNameClientLoginLogout,
		FuncNameUnregister,
	} {
		if s == "" {
			t.Fatal("empty func name constant")
		}
	}
}
