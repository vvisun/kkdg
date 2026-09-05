package hubtcp

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor"
)

func waitClientUnauthed(t *testing.T, c *HubClient, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !c.anyConnAuthed() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("hub client still authed after server stopped")
}

func TestHubClient_StopBeforeStart(t *testing.T) {
	af := kkactor.NewSilentActorFramework()
	ni := kkapp.NewNodeInfo("hubstop1", "game", "", "127.0.0.1:9101")
	cli := NewHubClient(ApplyClientOptions(WithAddr(freeTCPAddr(t))), af, ni)

	if err := cli.Stop(); err != nil {
		t.Fatalf("Stop before Start: %v", err)
	}
}

func TestHubClient_StopAfterFailedStart(t *testing.T) {
	af := kkactor.NewSilentActorFramework()
	ni := kkapp.NewNodeInfo("hubstop2", "game", "", "127.0.0.1:9102")
	// 无人监听的地址，Connect 必失败。
	cli := NewHubClient(
		ApplyClientOptions(
			WithAddr(freeTCPAddr(t)),
			WithClientReconnect(false, 0, 0, 0),
		),
		af,
		ni,
	)

	if err := cli.Start(); err == nil {
		t.Fatal("Start against a dead address should fail")
	}
	if err := cli.Stop(); err != nil {
		t.Fatalf("Stop after failed Start: %v", err)
	}
}

// Hub 断开重连后，服务端已把该连接上的 actor 全部注销，客户端必须凭本地清单补回。
func TestHubClient_ReconnectReregistersOwnedActors(t *testing.T) {
	addr := freeTCPAddr(t)
	const pwd = "hubtcp_reconnect_pwd"

	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}

	af := kkactor.NewSilentActorFramework()
	ni := kkapp.NewNodeInfo("hubrc1", "game", "", "127.0.0.1:9103")
	cli := NewHubClient(
		ApplyClientOptions(
			WithAddr(addr),
			WithPassword(pwd),
			WithClientReconnect(true, 50*time.Millisecond, 200*time.Millisecond, -1),
		),
		af,
		ni,
	)
	if err := cli.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Stop() }()

	waitClientAuthed(t, cli, 3*time.Second)

	aid, err := kkactor.NewLucencyID("hubrc1", "persistent")
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.ReqRegisterActor(aid); err != nil {
		t.Fatalf("ReqRegisterActor: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := serverRemoteMgr(t, srv).FindActor(aid); err != nil {
		t.Fatalf("actor not registered before restart: %v", err)
	}

	_ = srv.Stop()
	waitClientUnauthed(t, cli, 3*time.Second)

	srv2 := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv2.Start(); err != nil {
		t.Fatalf("restart hub server: %v", err)
	}
	defer func() { _ = srv2.Stop() }()

	waitClientAuthed(t, cli, 5*time.Second)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := serverRemoteMgr(t, srv2).FindActor(aid); err == nil {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatal("actor was not re-registered on the new hub after reconnect")
}

// 断开时在途请求必须清空，否则每断一次就积压一批永远收不到响应的请求。
func TestHubClient_DisconnectClearsPendingRequests(t *testing.T) {
	addr := freeTCPAddr(t)
	const pwd = "hubtcp_pending_pwd"

	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}

	af := kkactor.NewSilentActorFramework()
	ni := kkapp.NewNodeInfo("hubrc2", "game", "", "127.0.0.1:9104")
	cli := NewHubClient(
		ApplyClientOptions(
			WithAddr(addr),
			WithPassword(pwd),
			WithClientReconnect(false, 0, 0, 0),
		),
		af,
		ni,
	)
	if err := cli.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Stop() }()

	waitClientAuthed(t, cli, 3*time.Second)

	aid, _ := kkactor.NewLucencyID("hubrc2", "pending")
	if err := cli.ReqRegisterActor(aid); err != nil {
		t.Fatalf("ReqRegisterActor: %v", err)
	}

	_ = srv.Stop()
	waitClientUnauthed(t, cli, 3*time.Second)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		cli.muReqMap.Lock()
		n := len(cli.reqMap)
		cli.muReqMap.Unlock()
		if n == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("reqMap not cleared after disconnect")
}
