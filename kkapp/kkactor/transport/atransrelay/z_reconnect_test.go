package atransrelay

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actortrans"
)

func waitConnected(t *testing.T, tr *Transport, want bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if tr.connected() == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("transport connected=%v, want %v (nodeId=%s)", tr.connected(), want, tr.nodeID)
}

// Hub 重启后，传输层必须重新注册并恢复收发；否则 TCP 连上了 Hub 也不知道该节点是谁。
func TestRelay_ReconnectReregistersAndRecovers(t *testing.T) {
	addr := freeTCPAddr(t)
	hub := NewHub(addr)
	if err := hub.Start(); err != nil {
		t.Fatal(err)
	}

	reg := testRegistry(t)
	tr2 := NewTransport("nodeB", reg, Options{HubAddr: addr})
	tr1 := NewTransport("nodeA", reg, Options{HubAddr: addr})
	tr2.SetReceiver(&testRecv{ch: make(chan struct{}, 1)})

	if err := tr2.Start(); err != nil {
		t.Fatal(err)
	}
	if err := tr1.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = tr1.Close()
		_ = tr2.Close()
	}()

	target := actortrans.ActorRef{NodeID: "nodeB", ActorKey: "actor1"}
	if _, err := tr1.Request(target, &hpMsg{S: "req"}, 2*time.Second); err != nil {
		t.Fatalf("request before restart: %v", err)
	}

	_ = hub.Stop()
	waitConnected(t, tr1, false, 3*time.Second)
	waitConnected(t, tr2, false, 3*time.Second)

	hub2 := NewHub(addr)
	if err := hub2.Start(); err != nil {
		t.Fatalf("restart hub: %v", err)
	}
	defer func() { _ = hub2.Stop() }()

	waitConnected(t, tr1, true, 10*time.Second)
	waitConnected(t, tr2, true, 10*time.Second)

	deadline := time.Now().Add(5 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, lastErr = tr1.Request(target, &hpMsg{S: "req"}, 2*time.Second); lastErr == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("request never recovered after hub restart: %v", lastErr)
}

// Close 必须真正关掉底层客户端。断线后 Close，若 client 没关，kknet 会继续重连并重新注册。
func TestRelay_CloseAfterDisconnectStopsReconnect(t *testing.T) {
	addr := freeTCPAddr(t)
	hub := NewHub(addr)
	if err := hub.Start(); err != nil {
		t.Fatal(err)
	}

	reg := testRegistry(t)
	tr := NewTransport("nodeA", reg, Options{HubAddr: addr})
	tr.SetReceiver(&testRecv{ch: make(chan struct{}, 1)})
	if err := tr.Start(); err != nil {
		t.Fatal(err)
	}

	// 先断开：OnClose 会触发，此后再 Close。
	_ = hub.Stop()
	waitConnected(t, tr, false, 3*time.Second)

	if err := tr.Close(); err != nil {
		t.Fatalf("Close after disconnect: %v", err)
	}
	if atomic.LoadInt32(&tr.started) != 0 {
		t.Fatal("started should be reset by Close")
	}
	if tr.client != nil && tr.client.IsConnected() {
		t.Fatal("underlying client still connected after Close")
	}

	// Hub 回来后，已 Close 的传输层不得复活。
	hub2 := NewHub(addr)
	if err := hub2.Start(); err != nil {
		t.Fatalf("restart hub: %v", err)
	}
	defer func() { _ = hub2.Stop() }()

	time.Sleep(2 * time.Second)
	if tr.connected() {
		t.Fatal("closed transport reconnected and re-registered")
	}
	if atomic.LoadInt32(&tr.registered) != 0 {
		t.Fatal("closed transport re-registered after hub restart")
	}
}

// Start 连不上 Hub 时必须报错，且不留下 started 标记。
func TestRelay_StartAgainstDeadHubFails(t *testing.T) {
	tr := NewTransport("nodeA", testRegistry(t), Options{HubAddr: freeTCPAddr(t)})
	if err := tr.Start(); err == nil {
		t.Fatal("Start against a dead hub should fail")
	}
	if atomic.LoadInt32(&tr.started) != 0 {
		t.Fatal("started must be reset when Start fails")
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("Close after failed Start: %v", err)
	}
	// Start 失败时 kknet 可能已抢先把状态切到 Reconnecting 并拉起无限重连循环，
	// 该循环只在每轮开头看到 Closing/Closed 才退出，所以 Close 必须把状态推到 Closed。
	// 注：失败连接与 startReconnect 之间本身有竞争，重连循环不一定每次都被拉起，
	// 因此这里是一道部分防线，不是确定性的判别器。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if tr.client == nil || tr.client.IsStopped() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("orphan client still reconnecting after failed Start + Close")
}
