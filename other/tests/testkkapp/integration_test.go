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

type (
	MsgTest1 struct {
		ID   int
		Data string
	}
	MsgTest2 struct {
		ID   int
		Data string
	}
	MsgTest3 struct {
		ID   int
		Data string
	}
)

func InitMsgs(router *kkpacket.MsgRouter) {
	_ = router.Register(1, &MsgTest1{}, "logic")
	_ = router.Register(2, &MsgTest2{}, "logic")
	_ = router.Register(3, &MsgTest3{}, "logic")
}

type gameHandler struct {
	transportor gametrans.ITransportor
}

func (h *gameHandler) onMsgTest1(sessionID string, msg *MsgTest1) error {
	kklog.Infof("收到rpc消息 onMsgTest1: %v", msg)
	h.transportor.SendToClient(sessionID, msg)
	return nil
}

func (h *gameHandler) onMsgTest2(sessionID string, msg *MsgTest2) error {
	kklog.Infof("收到rpc消息 onMsgTest2: %v", msg)
	return nil
}

func (h *gameHandler) onMsgTest3(sessionID string, msg *MsgTest3) error {
	kklog.Infof("收到rpc消息 onMsgTest3: %v", msg)
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
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgTest1)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgTest2)
	msgreceiver.RegisterMsgHandler(msgReceiver, gh.onMsgTest3)

	t.Cleanup(func() { _ = gameApp.Stop() })

	// 等待 discovery 建立（requestAllMembers 约 1s 后触发，再留时间收响应）
	time.Sleep(2 * time.Second)

	//----------------------------- client -----------------------------

	// 客户端：发送 payload，期望 game 回显相同内容
	payload := []byte("hello")

	var recvMu sync.Mutex
	var recvData []byte
	recvCh := make(chan struct{})

	clientAppOpts := kkapp.ApplyOptions()
	InitMsgs(clientAppOpts.ClientMsgPacket.GetRouter())
	streamTool := clientAppOpts.StreamTool

	handler := &clientHandler{
		onRaw: func(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
			msg, e := kkpacket.DecodeStream(data, streamTool, clientAppOpts.ClientMsgPacket)

			if e != nil {
				t.Logf("unpack recv: %v", e)
				return
			}
			msgTest1, ok := msg.(*MsgTest1)
			if !ok {
				t.Logf("unpack recv: %v", e)
				return
			}
			recvMu.Lock()
			recvData = append([]byte(nil), msgTest1.Data...)
			recvMu.Unlock()
			select {
			case recvCh <- struct{}{}:
			default:
			}
		},
	}

	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(handler),
	)
	client := kktcp.NewClient(tcpAddr, handler, opts)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	time.Sleep(200 * time.Millisecond)

	bb, err := kkpacket.EncodeStream(
		&MsgTest1{ID: 1, Data: string(payload)},
		streamTool,
		clientAppOpts.ClientMsgPacket,
	)
	if err != nil {
		t.Fatalf("pack: %v", err)
	}
	if err := client.SendBuffer(bb); err != nil {
		t.Fatalf("send: %v", err)
	}

	select {
	case <-recvCh:
		recvMu.Lock()
		got := string(recvData)
		recvMu.Unlock()
		kklog.Infof("收到rpc消息 recv = %q, want %q", got, string(payload))
		if got != string(payload) {
			t.Errorf("收到rpc消息 recv = %q, want %q", got, string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for echo")
	}
}

type clientHandler struct {
	onRaw func(kknet.CONN_ID, *kkbuffer.ByteBuffer)
}

func (h *clientHandler) OnConnect(kknet.IConn)      {}
func (h *clientHandler) OnClose(kknet.IConn, error) {}

func (h *clientHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if h.onRaw != nil {
		h.onRaw(connID, data)
	} else {
		kkbuffer.Put(data)
	}
}
