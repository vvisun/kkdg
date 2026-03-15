// Package testkkapp 提供 kkapp 的集成测试。
// 需要本地 NATS 服务 (127.0.0.1:4222)，否则测试会 Skip。
package testkkapp

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate"
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/kknet/msgreceiver"
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

// TestIntegration_GateGame_Echo 集成测试：gate + game 节点，客户端连 gate 发消息，经 game 回显，验证收到
func TestIntegration_GateGame_Echo(t *testing.T) {
	natsURL := requireNATS(t)
	tcpAddr := freePort(t)
	rpcAddr := freePort(t)

	// 可以在这里调整传输层类型
	// const transType = transport.TransTypeRpc
	// const transType = transport.TransTypeNats
	// const transType = transport.TransTypeShard
	const transType = transport.TransTypeRpc

	appOpts := kkapp.ApplyOptions()

	//----------------------------- gate -----------------------------

	// gate 节点
	gateNode := kkapp.NewNodeInfo("gate1", kkapp.NodeTypeGate, tcpAddr, "", nil)
	gateApp := component.NewApplication(gateNode, nil, appOpts)
	InitMsgs(gateApp.GetOptions().ClientMsgPacket.GetRouter())
	gateOpt := ccgate.Option{
		TCPAddr:       tcpAddr,
		RpcAddr:       rpcAddr,
		DiscoveryUrl:  natsURL,
		ClusterUrl:    natsURL,
		LogicNodeType: kkapp.NodeTypeLogic,
		TransType:     transType,
	}
	gate := ccgate.NewGateComponent(gateOpt, kknet.DefaultOptions())
	if err := gateApp.AddComponent(gate); err != nil {
		t.Fatalf("add gate: %v", err)
	}
	if err := gateApp.Start(); err != nil {
		t.Fatalf("gate start: %v", err)
	}
	t.Cleanup(func() { _ = gateApp.Stop() })

	//----------------------------- game -----------------------------

	// game 节点（nodeType 必须为 logic 以匹配 gate 的 LogicNodeType）
	gameNode := kkapp.NewNodeInfo("game1", kkapp.NodeTypeLogic, "127.0.0.1:0", "", nil)
	gameApp := component.NewApplication(gameNode, nil, appOpts)
	InitMsgs(gameApp.GetOptions().TransMsgPacket.GetRouter())
	game := ccgame.NewGameComponent(ccgame.Option{
		TransType:    transType,
		RpcAddr:      rpcAddr,
		DiscoveryUrl: natsURL,
		ClusterUrl:   natsURL,
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

	var recvMu sync.Mutex
	clientRecvCh := make(chan struct{})

	clientAppOpts := kkapp.ApplyOptions()
	InitMsgs(clientAppOpts.ClientMsgPacket.GetRouter())
	opts := kknet.ApplyOptions(
		kknet.WithStreamTool(clientAppOpts.StreamTool),
		kknet.WithMsgPacket(clientAppOpts.ClientMsgPacket),
	)

	client1, err := newTestClient(t, 1, string(payload), clientRecvCh, opts, tcpAddr)
	if err != nil {
		t.Fatalf("new test client: %v", err)
	}
	client2, err := newTestClient(t, 2, string(payload), nil, opts, tcpAddr)
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

	if err := client2.client.SendMsg(&LoginReq{UserId: 2, Password: string(payload)}); err != nil {
		t.Fatalf("send: %v", err)
	}
	if err := client2.client.SendMsg(&MsgCounter{Seq: 1, Data: string(payload)}); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case <-client1.recvCh:
		recvMu.Lock()
		got := string(client1.recvData)
		recvMu.Unlock()
		if got != string(payload) {
			t.Errorf("客户端[%d]收到消息 recv = %q, want %q", client1.userId, got, string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("timeout waiting for echo from client[%d]", client1.userId)
	}
}

//---------------客户端处理-----------------------------------

type testClient struct {
	client   kknet.IClient
	userId   int64
	password string
	hasLogin bool
	recvCh   chan struct{}
	recvData []byte
}

func newTestClient(
	t *testing.T,
	userId int64,
	password string,
	recvCh chan struct{},
	opts kknet.Options,
	tcpAddr string,
) (*testClient, error) {

	cliInfo := &testClient{
		userId:   userId,
		password: password,
		hasLogin: false,
		recvCh:   recvCh,
		recvData: make([]byte, 0),
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
				if info.UserId != userId {
					t.Errorf("userId mismatch: got %d, want %d", info.UserId, userId)
				}
				cliInfo.hasLogin = true
			case *MsgCounter:
				cliInfo.recvData = append([]byte(nil), info.Data...)
				if cliInfo.recvCh != nil {
					select {
					case recvCh <- struct{}{}:
					default:
					}
				}
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
