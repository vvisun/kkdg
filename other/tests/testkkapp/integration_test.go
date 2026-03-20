// Package testkkapp 提供 kkapp 的集成测试。
// 需要本地 NATS 服务 (127.0.0.1:4222)，否则测试会 Skip。
package testkkapp

import (
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/msgreceiver"
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kkoption"
)

func requireNATS(t *testing.T) string {
	nc, err := net.DialTimeout("tcp", "127.0.0.1:4222", 500*time.Millisecond)
	if err != nil {
		t.Skipf("NATS not available at 127.0.0.1:4222: %v", err)
	}
	nc.Close()
	return "nats://127.0.0.1:4222"
}

func freePort(t *testing.T) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

//---------------消息定义-----------------------------------

type (
	LoginReq struct {
		UserId   int64
		Password string
	}
	LoginResp struct {
		UserId   int64
		UserData string
	}
	KickOutPush struct {
		UserId int64
		Reason string
	}
	MsgCounter struct {
		Seq  int
		Data string
	}
)

func InitMsgs(router *kkpacket.MsgRouter) {
	_ = router.Register(1, &LoginReq{}, "logic")
	_ = router.Register(2, &LoginResp{}, "logic")
	_ = router.Register(3, &KickOutPush{}, "logic")
	_ = router.Register(4, &MsgCounter{}, "logic")
}

//---------------游戏逻辑处理-----------------------------------

type gameHandler struct {
	transportor gametrans.ITransportor
}

func (h *gameHandler) onLoginReq(sessionID string, msg *LoginReq) error {
	kklog.Infof("逻辑服收到消息: type = %T, data = %v", msg, msg)
	resp := &LoginResp{
		UserId:   msg.UserId,
		UserData: "user data",
	}
	h.transportor.NotifyClientLoginLogout(sessionID, msg.UserId, true)
	h.transportor.SendToClient(sessionID, resp)
	return nil
}

func (h *gameHandler) onLoginResp(sessionID string, msg *LoginResp) error {
	kklog.Infof("逻辑服收到消息: type = %T, data = %v", msg, msg)
	return nil
}

func (h *gameHandler) onMsgCounter(sessionID string, msg *MsgCounter) error {
	kklog.Infof("逻辑服收到消息: type = %T, data = %v", msg, msg)
	resp := &MsgCounter{
		Seq:  msg.Seq + 1,
		Data: msg.Data,
	}
	h.transportor.SendToClient(sessionID, resp)
	return nil
}

func TestIntegration_GateGame_Echo_Shard(t *testing.T) {
	runIntegration_GateGame_Echo(t, transport.TransTypeShard)
}

func TestIntegration_GateGame_Echo_Rpc(t *testing.T) {
	runIntegration_GateGame_Echo(t, transport.TransTypeRpc)
}

func TestIntegration_GateGame_Echo_Nats(t *testing.T) {
	runIntegration_GateGame_Echo(t, transport.TransTypeNats)
}

// runIntegration_GateGame_Echo 集成测试：gate + game 节点，客户端连 gate 发消息，经 game 回显，验证收到
func runIntegration_GateGame_Echo(t *testing.T, transType transport.TransType) {
	natsURL := requireNATS(t)
	tcpAddr := freePort(t)
	rpcAddr := freePort(t)

	appOpts := kkapp.ApplyOptions()

	//----------------------------- gate -----------------------------

	// gate 节点
	gateNode := kkapp.NewNodeInfo("gate1", kkapp.NodeTypeGate, tcpAddr, "")
	afGate := kkactor.NewActorFramework()
	gateApp := component.NewApplication(gateNode, afGate, appOpts)
	InitMsgs(gateApp.GetOptions().ClientMsgPacket.GetRouter())
	gateOpt := ccgate.Options{
		TCPAddr:         tcpAddr,
		TransServerAddr: rpcAddr,
		DiscoveryUrl:    natsURL,
		ClusterUrl:      natsURL,
		TransType:       transType,
	}
	gate := ccgate.NewGateComponent(gateOpt, kknet.DefaultOptions())
	if err := gateApp.AddComponent(gate); err != nil {
		t.Fatalf("add gate: %v", err)
	}
	if err := gateApp.Start(); err != nil {
		t.Fatalf("gate start: %v", err)
	}
	t.Cleanup(func() { _ = gateApp.Stop() })

	gate.SetErrCallback(func(conn kknet.IConn, errCode ccgate.GateErrorCode) {
		switch errCode {
		case ccgate.ERR_RECV_QUEUE_FULL:
			conn.SendMsg(&KickOutPush{UserId: 0, Reason: "服务器繁忙"})
			conn.Close()
		case ccgate.ERR_USER_KICKED:
			conn.SendMsg(&KickOutPush{UserId: 0, Reason: "被顶号"})
			conn.Close()
		}
	})

	//----------------------------- game -----------------------------

	// game 节点（nodeType 必须为 logic 以匹配 gate 的 LogicNodeType）
	gameNode := kkapp.NewNodeInfo("game1", kkapp.NodeTypeLogic, "127.0.0.1:0", "")
	afGame := kkactor.NewActorFramework()
	gameApp := component.NewApplication(gameNode, afGame, appOpts)
	InitMsgs(gameApp.GetOptions().ClientMsgPacket.GetRouter())
	game := ccgame.NewGameComponent(ccgame.Options{
		TransType:       transType,
		TransServerAddr: rpcAddr,
		DiscoveryUrl:    natsURL,
		ClusterUrl:      natsURL,
	})

	if err := gameApp.AddComponent(game); err != nil {
		t.Fatalf("add game: %v", err)
	}
	if err := gameApp.Start(); err != nil {
		t.Fatalf("game start: %v", err)
	}

	msgReceiver := game.GetMsgReceiver()
	gh := &gameHandler{transportor: game.GetTransportor()}
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onLoginReq)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onLoginResp)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgCounter)

	t.Cleanup(func() { _ = gameApp.Stop() })

	// 等待 discovery 建立（requestAllMembers 约 1s 后触发，再留时间收响应）
	time.Sleep(2 * time.Second)

	//----------------------------- client -----------------------------

	// 客户端：发送 payload，期望 game 回显相同内容
	payload := []byte("hello")

	clientAppOpts := kkapp.ApplyOptions()
	InitMsgs(clientAppOpts.ClientMsgPacket.GetRouter())
	opts := kknet.ApplyOptions(
		kknet.WithStreamTool(clientAppOpts.StreamTool),
		kknet.WithMsgPacket(clientAppOpts.ClientMsgPacket),
	)

	client1, err := newTestClient(t, 1, string(payload), opts, tcpAddr)
	if err != nil {
		t.Fatalf("new test client: %v", err)
	}
	client2, err := newTestClient(t, 2, string(payload), opts, tcpAddr)
	if err != nil {
		t.Fatalf("new test client: %v", err)
	}

	t.Cleanup(func() {
		_ = client1.client.Close()
		_ = client2.client.Close()
	})

	time.Sleep(200 * time.Millisecond)

	if err := client1.client.SendMsg(&LoginReq{UserId: 1, Password: string(payload)}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := client1.client.SendMsg(&MsgCounter{Seq: 1, Data: string(payload)}); err != nil {
		t.Fatalf("send: %v", err)
	}

	if err := client2.client.SendMsg(&LoginReq{UserId: 1, Password: string(payload)}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := client2.client.SendMsg(&MsgCounter{Seq: 1, Data: string(payload)}); err != nil {
		t.Fatalf("send: %v", err)
	}

	time.Sleep(3 * time.Second)
}

//---------------客户端处理-----------------------------------

type testClient struct {
	client   kknet.IClient
	clientID int64
	password string
	hasLogin bool
}

func newTestClient(
	t *testing.T,
	userId int64,
	password string,
	opts kknet.Options,
	tcpAddr string,
) (*testClient, error) {

	cliInfo := &testClient{
		clientID: userId,
		password: password,
		hasLogin: false,
	}

	handler := &clientHandler{
		onRaw: func(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
			msg, e := kkpacket.DecodeStream(data, opts.StreamTool, opts.WpOptions.MsgPacket)

			if e != nil {
				t.Logf("unpack recv: %v", e)
				return
			}

			kklog.Infof("客户端[%d]收到消息: %#v", userId, msg)

			switch info := msg.(type) {
			case *LoginResp:
				cliInfo.hasLogin = true
			case *MsgCounter:

			case *KickOutPush:
				kklog.Infof("客户端[%d]收到顶号消息: %v", userId, info)
				cliInfo.client.Close()
			default:
				t.Logf("unknown message type: %T", msg)
			}
		},
	}

	kkoption.ApplyOptionsTo(&opts,
		kknet.WithRawHandler(handler),
		kknet.WithStreamTool(opts.StreamTool),
		kknet.WithMsgPacket(opts.WpOptions.MsgPacket),
	)
	client := kktcp.NewClient(tcpAddr, handler, opts)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
		return nil, err
	}

	cliInfo.client = client

	return cliInfo, nil
}

type clientHandler struct {
	onRaw func(kknet.CONN_ID, *kkbuffer.ByteBuffer)
}

func (h *clientHandler) OnConnect(kknet.IConn) {

}
func (h *clientHandler) OnClose(kknet.IConn, error) {

}

func (h *clientHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if h.onRaw != nil {
		h.onRaw(connID, data)
	} else {
		kkbuffer.Put(data)
	}
}
