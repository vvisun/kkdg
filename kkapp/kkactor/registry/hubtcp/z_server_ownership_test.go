package hubtcp

import (
	"fmt"
	"sync"
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

// 接管与旧连接断开并发时，无论谁先落地，最终目录里都必须留下该 actor。
// 归属判定与目录写不在同一临界区时，存在「旧连接判定自己仍是归属方 → 新连接写入目录
// → 旧连接才执行删除」的交错，会把刚注册好的 actor 抹掉。
func TestHubServer_ConcurrentHandoffKeepsActorRegistered(t *testing.T) {
	addr := freeTCPAddr(t)
	const pwd = "hubtcp_owner_pwd4"
	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	for i := 0; i < 12; i++ {
		aid, err := kkactor.NewLucencyID("hubown4", fmt.Sprintf("shared_%d", i))
		if err != nil {
			t.Fatal(err)
		}

		old := startOwnershipClient(t, addr, pwd, "hubown4", "127.0.0.1:9301")
		if err := old.ReqRegisterActor(aid); err != nil {
			t.Fatalf("iter %d old register: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)

		fresh := startOwnershipClient(t, addr, pwd, "hubown4", "127.0.0.1:9302")

		// 新连接注册与旧连接断开同时发生。
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			_ = fresh.ReqRegisterActor(aid)
		}()
		go func() {
			defer wg.Done()
			_ = old.Stop()
		}()
		wg.Wait()

		time.Sleep(400 * time.Millisecond)
		if _, err := serverRemoteMgr(t, srv).FindActor(aid); err != nil {
			_ = fresh.Stop()
			t.Fatalf("iter %d: actor lost after concurrent handoff: %v", i, err)
		}
		_ = fresh.Stop()
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
