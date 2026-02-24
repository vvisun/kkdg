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
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
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

// TestIntegration_GateGame_Echo 集成测试：gate + game 节点，客户端连 gate 发消息，经 game 回显，验证收到
func TestIntegration_GateGame_Echo(t *testing.T) {
	natsURL := requireNATS(t)
	tcpAddr := freePort(t)

	settings := map[string]string{"nats_url": natsURL}

	// gate 节点
	gateNode := kkapp.NewNodeInfo("gate1", "gate", tcpAddr, "", settings)
	gateApp := component.NewApplication(gateNode)
	gateOpt := ccgate.Option{
		TCPAddr:       tcpAddr,
		NatsURL:       natsURL,
		LogicNodeType: "logic",
	}
	gate := ccgate.NewGateComponent(gateOpt)
	if err := gateApp.AddComponent(gate); err != nil {
		t.Fatalf("add gate: %v", err)
	}
	if err := gateApp.Start(); err != nil {
		t.Fatalf("gate start: %v", err)
	}
	t.Cleanup(func() { _ = gateApp.Stop() })

	// game 节点（nodeType 必须为 logic 以匹配 gate 的 LogicNodeType）
	gameNode := kkapp.NewNodeInfo("game1", "logic", "127.0.0.1:0", "", settings)
	gameApp := component.NewApplication(gameNode)
	game := ccgame.NewGameComponent()
	if err := gameApp.AddComponent(game); err != nil {
		t.Fatalf("add game: %v", err)
	}
	if err := gameApp.Start(); err != nil {
		t.Fatalf("game start: %v", err)
	}
	t.Cleanup(func() { _ = gameApp.Stop() })

	// 等待 discovery 建立（requestAllMembers 约 1s 后触发，再留时间收响应）
	time.Sleep(2 * time.Second)

	// 客户端：发送 payload，期望 game 回显相同内容
	payload := []byte("hello")

	var recvMu sync.Mutex
	var recvData []byte
	recvCh := make(chan struct{})

	handler := &testRawHandler{
		onRaw: func(_ kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
			bytes, e := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
			kkbuffer.Put(data)
			if e != nil {
				t.Logf("unpack recv: %v", e)
				return
			}
			recvMu.Lock()
			recvData = append([]byte(nil), bytes...)
			recvMu.Unlock()
			select {
			case recvCh <- struct{}{}:
			default:
			}
		},
	}

	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Stdout()),
		kknet.WithRawHandler(handler),
	)
	client := kktcp.NewClient(tcpAddr, handler, opts)
	if err := client.Connect(); err != nil {
		t.Fatalf("client connect: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })

	time.Sleep(200 * time.Millisecond)

	bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
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
		if got != string(payload) {
			t.Errorf("recv = %q, want %q", got, string(payload))
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for echo")
	}
}

type testRawHandler struct {
	onRaw func(kknet.CONN_ID, *kkbuffer.ByteBuffer)
}

func (h *testRawHandler) OnConnect(kknet.IConn) {}
func (h *testRawHandler) OnClose(kknet.IConn, error) {}

func (h *testRawHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if h.onRaw != nil {
		h.onRaw(connID, data)
	} else {
		kkbuffer.Put(data)
	}
}
