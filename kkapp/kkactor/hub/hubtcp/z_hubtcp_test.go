package hubtcp

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/hub/actorhub"
	"github.com/vvisun/kkdg/kkapp/kkactor/hub/hubproto"
	"github.com/vvisun/kkdg/kkerrors"
)

func serverRemoteMgr(t *testing.T, srv *HubServer) *actorhub.RemoteActorMgr {
	t.Helper()
	m, ok := srv.ServerRemoteActorMgr().(*actorhub.RemoteActorMgr)
	if !ok {
		t.Fatal("ServerRemoteActorMgr is not *actorhub.RemoteActorMgr")
	}
	return m
}

func freeTCPAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func waitClientAuthed(t *testing.T, c *HubClient, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.anyConnAuthed() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("hub client not authed in time")
}

func TestHubTCP_RegisterFindServerAndClientCache(t *testing.T) {
	addr := freeTCPAddr(t)
	const pwd = "hubtcp_test_pwd"
	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	af := kkactor.NewSilentActorFramework()
	ni := kkapp.NewNodeInfo("hubnode1", "game", "127.0.0.1:1", "127.0.0.1:9001")
	cli := NewHubClient(
		ApplyClientOptions(WithAddr(addr), WithPassword(pwd)),
		af,
		ni,
	)
	if err := cli.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Stop() }()

	waitClientAuthed(t, cli, 3*time.Second)

	aid, err := kkactor.NewLucencyID("hubnode1", "actor_one")
	if err != nil {
		t.Fatal(err)
	}
	if err := cli.RegisterActor(aid); err != nil {
		t.Fatalf("RegisterActor: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	if _, err := serverRemoteMgr(t, srv).FindActor(aid); err != nil {
		t.Fatalf("server FindActor after register: %v", err)
	}

	if err := cli.FindActor(aid); err != nil {
		t.Fatalf("FindActor send: %v", err)
	}
	time.Sleep(150 * time.Millisecond)

	ra, err := cli.GetRemoteActorMgr().FindActor(aid)
	if err != nil {
		t.Fatalf("client cache FindActor: %v", err)
	}
	if ra.RpcAddress() != "127.0.0.1:9001" {
		t.Fatalf("RpcAddress = %q, want 127.0.0.1:9001", ra.RpcAddress())
	}
}

func TestHubTCP_GetAllActorsOfNode(t *testing.T) {
	addr := freeTCPAddr(t)
	const pwd = "hubtcp_test_pwd2"
	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	af := kkactor.NewSilentActorFramework()
	ni := kkapp.NewNodeInfo("hubnode2", "game", "", "127.0.0.1:9002")
	cli := NewHubClient(
		ApplyClientOptions(WithAddr(addr), WithPassword(pwd)),
		af,
		ni,
	)
	if err := cli.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Stop() }()

	waitClientAuthed(t, cli, 3*time.Second)

	a1, _ := kkactor.NewLucencyID("hubnode2", "a1")
	a2, _ := kkactor.NewLucencyID("hubnode2", "a2")
	_ = cli.RegisterActor(a1)
	_ = cli.RegisterActor(a2)
	time.Sleep(200 * time.Millisecond)

	if err := cli.GetAllActorsOfNode("hubnode2"); err != nil {
		t.Fatalf("GetAllActorsOfNode: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	list := cli.GetRemoteActorMgr().GetAllActorsOfNode("hubnode2")
	if len(list) != 2 {
		t.Fatalf("GetAllActorsOfNode cache len = %d, want 2", len(list))
	}
}

func TestHubTCP_WrongPasswordNotAuthed(t *testing.T) {
	addr := freeTCPAddr(t)
	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword("right")))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	af := kkactor.NewSilentActorFramework()
	ni := kkapp.NewNodeInfo("hubnode3", "game", "", "127.0.0.1:1")
	cli := NewHubClient(
		ApplyClientOptions(WithAddr(addr), WithPassword("wrong")),
		af,
		ni,
	)
	if err := cli.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cli.Stop() }()

	time.Sleep(200 * time.Millisecond)
	if cli.anyConnAuthed() {
		t.Fatal("client should not be authed with wrong password")
	}

	aid, _ := kkactor.NewLucencyID("hubnode3", "x")
	err := cli.RegisterActor(aid)
	if !errors.Is(err, hubproto.ErrNotAuthed) {
		t.Fatalf("RegisterActor err = %v, want ErrNotAuthed", err)
	}
}

func TestHubTCP_ClientDisconnectUnregistersOnServer(t *testing.T) {
	addr := freeTCPAddr(t)
	const pwd = "hubtcp_test_pwd3"
	srv := NewHubServer(ApplyServerOptions(WithServerAddr(addr), WithServerPassword(pwd)))
	if err := srv.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = srv.Stop() }()

	af := kkactor.NewSilentActorFramework()
	ni := kkapp.NewNodeInfo("hubnode4", "game", "", "127.0.0.1:1")
	cli := NewHubClient(
		ApplyClientOptions(WithAddr(addr), WithPassword(pwd)),
		af,
		ni,
	)
	if err := cli.Start(); err != nil {
		t.Fatal(err)
	}
	waitClientAuthed(t, cli, 3*time.Second)

	aid, _ := kkactor.NewLucencyID("hubnode4", "ephemeral")
	_ = cli.RegisterActor(aid)
	time.Sleep(150 * time.Millisecond)
	if _, err := serverRemoteMgr(t, srv).FindActor(aid); err != nil {
		t.Fatalf("before close: %v", err)
	}

	_ = cli.Stop()

	deadline := time.Now().Add(3 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		_, lastErr = serverRemoteMgr(t, srv).FindActor(aid)
		if lastErr != nil && errors.Is(lastErr, kkerrors.ErrActorNotFound) {
			return
		}
		time.Sleep(30 * time.Millisecond)
	}
	t.Fatalf("after client stop, actor still registered or wrong err: %v", lastErr)
}
