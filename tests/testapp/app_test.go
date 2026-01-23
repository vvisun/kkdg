package app_test

import (
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/comps/ccclient"
	"github.com/vvisun/kkdg/kkapp/comps/ccgame"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate"
)

func freeTCPAddr(t testing.TB) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func TestAppIntegration(t *testing.T) {
	// 获取空闲端口
	tcpAddr := freeTCPAddr(t)
	wsAddr := freeTCPAddr(t)

	// 创建应用
	nodeInfo := kkapp.NewNodeInfo("testapp", "testapp", tcpAddr, wsAddr, nil)
	app := component.NewApplication(nodeInfo)

	// 创建游戏组件
	gameComp := ccgame.NewGameComponent()
	if err := gameComp.Init(); err != nil {
		t.Fatalf("game init: %v", err)
	}
	if err := app.AddCompenent(gameComp); err != nil {
		t.Fatalf("add game: %v", err)
	}

	// 创建网关组件，注入业务处理器
	gateComp := ccgate.NewGateComponent(ccgate.Option{
		TCPAddr:         tcpAddr,
		WSAddr:          wsAddr,
		BusinessHandler: gameComp,
	})
	if err := gateComp.Init(); err != nil {
		t.Fatalf("gate init: %v", err)
	}
	if err := app.AddCompenent(gateComp); err != nil {
		t.Fatalf("add gate: %v", err)
	}

	// 创建发现组件
	// discoveryComp := kkdiscovery.NewCompDiscovery(kkdiscovery.NewNatsDiscovery("test", nodeInfo, nil))
	// if err := discoveryComp.Init(); err != nil {
	// 	t.Fatalf("discovery init: %v", err)
	// }
	// if err := app.AddCompenent(discoveryComp); err != nil {
	// 	t.Fatalf("add discovery: %v", err)
	// }

	// 启动应用
	if err := app.Start(); err != nil {
		t.Fatalf("app start: %v", err)
	}
	defer func() {
		if err := app.Stop(); err != nil {
			t.Errorf("app stop: %v", err)
		}
	}()

	// 等待服务器启动
	time.Sleep(200 * time.Millisecond)

	// 创建客户端组件（TCP）
	clientComp := ccclient.NewClientComponent(ccclient.Option{
		TCPAddr: tcpAddr,
	})
	if err := clientComp.Init(); err != nil {
		t.Fatalf("client init: %v", err)
	}
	if err := clientComp.Start(); err != nil {
		t.Fatalf("client start: %v", err)
	}

	// 等待消息处理
	time.Sleep(5000 * time.Millisecond)

	// 停止客户端
	if err := clientComp.Stop(); err != nil {
		t.Errorf("client stop: %v", err)
	}
}

func TestAppWebSocket(t *testing.T) {
	// 获取空闲端口
	tcpAddr := freeTCPAddr(t)
	wsAddr := freeTCPAddr(t)

	// 创建应用
	nodeInfo := kkapp.NewNodeInfo("testapp", "testapp", tcpAddr, wsAddr, nil)
	app := component.NewApplication(nodeInfo)

	// 创建游戏组件
	gameComp := ccgame.NewGameComponent()
	if err := gameComp.Init(); err != nil {
		t.Fatalf("game init: %v", err)
	}
	if err := app.AddCompenent(gameComp); err != nil {
		t.Fatalf("add game: %v", err)
	}

	// 创建网关组件，注入业务处理器
	gateComp := ccgate.NewGateComponent(ccgate.Option{
		TCPAddr:         tcpAddr,
		WSAddr:          wsAddr,
		BusinessHandler: gameComp,
	})
	if err := gateComp.Init(); err != nil {
		t.Fatalf("gate init: %v", err)
	}
	if err := app.AddCompenent(gateComp); err != nil {
		t.Fatalf("add gate: %v", err)
	}

	// 启动应用
	if err := app.Start(); err != nil {
		t.Fatalf("app start: %v", err)
	}
	defer func() {
		if err := app.Stop(); err != nil {
			t.Errorf("app stop: %v", err)
		}
	}()

	// 等待服务器启动
	time.Sleep(100 * time.Millisecond)

	// 创建 WebSocket URL
	wsURL := "ws://" + wsAddr + "/ws"

	// 创建客户端组件（WebSocket）
	clientComp := ccclient.NewClientComponent(ccclient.Option{
		WSURL: wsURL,
	})
	if err := clientComp.Init(); err != nil {
		t.Fatalf("client init: %v", err)
	}
	if err := clientComp.Start(); err != nil {
		t.Fatalf("client start: %v", err)
	}

	// 等待消息处理
	time.Sleep(500 * time.Millisecond)

	// 停止客户端
	if err := clientComp.Stop(); err != nil {
		t.Errorf("client stop: %v", err)
	}
}

func TestAppMultipleClients(t *testing.T) {
	// 获取空闲端口
	tcpAddr := freeTCPAddr(t)

	// 创建应用
	nodeInfo := kkapp.NewNodeInfo("testapp", "testapp", tcpAddr, "", nil)
	app := component.NewApplication(nodeInfo)

	// 创建游戏组件
	gameComp := ccgame.NewGameComponent()
	if err := gameComp.Init(); err != nil {
		t.Fatalf("game init: %v", err)
	}
	if err := app.AddCompenent(gameComp); err != nil {
		t.Fatalf("add game: %v", err)
	}

	// 创建网关组件，注入业务处理器
	gateComp := ccgate.NewGateComponent(ccgate.Option{
		TCPAddr:         tcpAddr,
		BusinessHandler: gameComp,
	})
	if err := gateComp.Init(); err != nil {
		t.Fatalf("gate init: %v", err)
	}
	if err := app.AddCompenent(gateComp); err != nil {
		t.Fatalf("add gate: %v", err)
	}

	// 启动应用
	if err := app.Start(); err != nil {
		t.Fatalf("app start: %v", err)
	}
	defer func() {
		if err := app.Stop(); err != nil {
			t.Errorf("app stop: %v", err)
		}
	}()

	// 等待服务器启动
	time.Sleep(100 * time.Millisecond)

	// 创建多个客户端
	clients := make([]*ccclient.ClientComponent, 3)
	for i := 0; i < 3; i++ {
		clientComp := ccclient.NewClientComponent(ccclient.Option{
			TCPAddr: tcpAddr,
		})
		if err := clientComp.Init(); err != nil {
			t.Fatalf("client %d init: %v", i, err)
		}
		if err := clientComp.Start(); err != nil {
			t.Fatalf("client %d start: %v", i, err)
		}
		clients[i] = clientComp
	}

	// 等待消息处理
	time.Sleep(500 * time.Millisecond)

	// 停止所有客户端
	for i, client := range clients {
		if err := client.Stop(); err != nil {
			t.Errorf("client %d stop: %v", i, err)
		}
	}
}
