package hubtcp

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor"
)

func startOwnershipClient(t *testing.T, addr, pwd, nodeID, rpcAddr string) *HubClient {
	t.Helper()
	cli := NewHubClient(
		ApplyClientOptions(
			WithAddr(addr),
			WithPassword(pwd),
			WithClientReconnect(false, 0, 0, 0),
		),
		kkactor.NewSilentActorFramework(),
		kkapp.NewNodeInfo(nodeID, "game", "", rpcAddr),
	)
	if err := cli.Start(); err != nil {
		t.Fatal(err)
	}
	waitClientAuthed(t, cli, 3*time.Second)
	return cli
}

// 同一个 actor 被新连接重新注册后，旧连接断开不得把它从目录里抹掉。
// 这是节点重连时的真实交错：旧连接的 OnClose 可能晚于新连接的注册到达。
func TestHubServer_StaleConnCloseKeepsReregisteredActor(t *testing.T) {
	addr := freeTCPAddr(t)
	const pwd = "hubtcp_owner_pwd"
	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	aid, err := kkactor.NewLucencyID("hubown1", "shared")
	if err != nil {
		t.Fatal(err)
	}

	// 旧连接注册。
	old := startOwnershipClient(t, addr, pwd, "hubown1", "127.0.0.1:9201")
	if err := old.ReqRegisterActor(aid); err != nil {
		t.Fatalf("old register: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := serverRemoteMgr(t, srv).FindActor(aid); err != nil {
		t.Fatalf("actor missing after old register: %v", err)
	}

	// 新连接接管同一个 actor。
	fresh := startOwnershipClient(t, addr, pwd, "hubown1", "127.0.0.1:9202")
	defer func() { _ = fresh.Stop() }()
	if err := fresh.ReqRegisterActor(aid); err != nil {
		t.Fatalf("fresh register: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	// 旧连接现在才断开。
	_ = old.Stop()
	time.Sleep(500 * time.Millisecond)

	ra, err := serverRemoteMgr(t, srv).FindActor(aid)
	if err != nil {
		t.Fatalf("stale conn close wiped the re-registered actor: %v", err)
	}
	if ra.RpcAddress() != "127.0.0.1:9202" {
		t.Fatalf("RpcAddress = %q, want the new connection's 127.0.0.1:9202", ra.RpcAddress())
	}
}

// 非归属连接发来的注销请求不得影响全局注册表。
func TestHubServer_UnregisterFromNonOwnerIsIgnored(t *testing.T) {
	addr := freeTCPAddr(t)
	const pwd = "hubtcp_owner_pwd2"
	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	aid, _ := kkactor.NewLucencyID("hubown2", "shared")

	old := startOwnershipClient(t, addr, pwd, "hubown2", "127.0.0.1:9203")
	defer func() { _ = old.Stop() }()
	if err := old.ReqRegisterActor(aid); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)

	fresh := startOwnershipClient(t, addr, pwd, "hubown2", "127.0.0.1:9204")
	defer func() { _ = fresh.Stop() }()
	if err := fresh.ReqRegisterActor(aid); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)

	// 旧连接（已非归属方）请求注销。
	if err := old.ReqUnregisterActor(aid); err != nil {
		t.Fatalf("old unregister send: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	if _, err := serverRemoteMgr(t, srv).FindActor(aid); err != nil {
		t.Fatalf("non-owner unregister removed the actor: %v", err)
	}
}

// 归属方自己注销时必须真的从目录里删掉。
func TestHubServer_OwnerUnregisterRemovesActor(t *testing.T) {
	addr := freeTCPAddr(t)
	const pwd = "hubtcp_owner_pwd3"
	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	aid, _ := kkactor.NewLucencyID("hubown3", "solo")
	cli := startOwnershipClient(t, addr, pwd, "hubown3", "127.0.0.1:9205")
	defer func() { _ = cli.Stop() }()

	if err := cli.ReqRegisterActor(aid); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	if _, err := serverRemoteMgr(t, srv).FindActor(aid); err != nil {
		t.Fatalf("register failed: %v", err)
	}

	if err := cli.ReqUnregisterActor(aid); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := serverRemoteMgr(t, srv).FindActor(aid); err == nil {
		t.Fatal("owner unregister did not remove the actor")
	}
}
