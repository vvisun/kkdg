package cnats

import (
	"sync"
	"testing"
	"time"

	"github.com/vvisun/kkdg/remotes/kkcluster"
)

// 无 kkdiscovery 时 ClusterOption.Discovery 为 nil：不校验成员是否存在，直接走 NATS。

func setupNoDiscoveryClusters(t *testing.T) (c1, c2 kkcluster.ICluster, cleanup func()) {
	t.Helper()
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	opt := kkcluster.ApplyOptions(kkcluster.WithUrl(natsURL))
	c1 = NewNatsCluster("node1", "typea", opt)
	c2 = NewNatsCluster("node2", "typea", opt)

	if err := c1.Start(); err != nil {
		t.Fatalf("cluster1.Start: %v", err)
	}
	if err := c2.Start(); err != nil {
		c1.Stop()
		t.Fatalf("cluster2.Start: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	cleanup = func() {
		c1.Stop()
		c2.Stop()
	}
	return c1, c2, cleanup
}

func TestNoDiscovery_PublishRemote(t *testing.T) {
	cluster1, cluster2, cleanup := setupNoDiscoveryClusters(t)
	defer cleanup()

	received := make(chan string, 1)
	cluster2.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {
		received <- nodeID + "|" + string(packet.ArgBytes)
	})

	packet := kkcluster.NewClusterPacket()
	packet.FuncName = "ping"
	packet.ArgBytes = []byte("hello")

	if err := cluster1.PublishRemote("node2", packet); err != nil {
		t.Fatalf("PublishRemote: %v", err)
	}

	select {
	case got := <-received:
		if got != "node1|hello" {
			t.Fatalf("handler got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for publish handler")
	}
}

func TestNoDiscovery_RequestRemote(t *testing.T) {
	cluster1, cluster2, cleanup := setupNoDiscoveryClusters(t)
	defer cleanup()

	cluster2.SetRequestHandler(func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error) {
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeSuccess),
			Data:      append([]byte(nil), req.Packet.ArgBytes...),
		}, nil
	})

	packet := kkcluster.NewClusterPacket()
	packet.FuncName = "echo"
	packet.ArgBytes = []byte("rpc")

	data, code := cluster1.RequestRemote("node2", packet, 3*time.Second)
	if code != kkcluster.ClusterErrorCodeSuccess {
		t.Fatalf("RequestRemote code = %v", code)
	}
	if string(data) != "rpc" {
		t.Fatalf("data = %q", string(data))
	}
}

func TestNoDiscovery_Request_NATSReply(t *testing.T) {
	cluster1, cluster2, cleanup := setupNoDiscoveryClusters(t)
	defer cleanup()

	nc1, ok := cluster1.(*NatsCluster)
	if !ok {
		t.Fatal("cluster1 is not *NatsCluster")
	}

	cluster2.SetRequestHandler(func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error) {
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeSuccess),
			Data:      []byte("nats-req"),
		}, nil
	})

	packet := kkcluster.NewClusterPacket()
	packet.FuncName = "t"
	packet.ArgBytes = []byte("x")

	data, code := nc1.request("node2", packet, 3*time.Second)
	if code != kkcluster.ClusterErrorCodeSuccess {
		t.Fatalf("request code = %v", code)
	}
	if string(data) != "nats-req" {
		t.Fatalf("data = %q", string(data))
	}
}

func TestNoDiscovery_RequestRemoteAsync(t *testing.T) {
	cluster1, cluster2, cleanup := setupNoDiscoveryClusters(t)
	defer cleanup()

	cluster2.SetRequestHandler(func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error) {
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeSuccess),
			Data:      []byte("async-ok"),
		}, nil
	})

	done := make(chan struct{})
	packet := kkcluster.NewClusterPacket()
	packet.FuncName = "a"
	packet.ArgBytes = []byte("b")

	err := cluster1.RequestRemoteAsync("node2", packet, func(data []byte, code kkcluster.ClusterErrorCode) {
		if code != kkcluster.ClusterErrorCodeSuccess {
			t.Errorf("code = %v", code)
		}
		if string(data) != "async-ok" {
			t.Errorf("data = %q", string(data))
		}
		close(done)
	}, 3*time.Second)
	if err != nil {
		t.Fatalf("RequestRemoteAsync: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("async callback timeout")
	}
}

func TestNoDiscovery_PublishRemoteType(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	opt := kkcluster.ApplyOptions(kkcluster.WithUrl(natsURL))
	c1 := NewNatsCluster("node1", "typea", opt)
	c2 := NewNatsCluster("node2", "typea", opt)
	c3 := NewNatsCluster("node3", "typeb", opt)

	for _, c := range []kkcluster.ICluster{c1, c2, c3} {
		if err := c.Start(); err != nil {
			t.Fatalf("Start: %v", err)
		}
	}
	defer func() {
		c1.Stop()
		c2.Stop()
		c3.Stop()
	}()

	time.Sleep(200 * time.Millisecond)

	var mu sync.Mutex
	var gotTypeA int
	handler := func(nodeID string, packet *kkcluster.ClusterPacket) {
		mu.Lock()
		gotTypeA++
		mu.Unlock()
		_ = nodeID
		_ = packet
	}
	c2.SetPublishHandler(handler)
	// typeb 不应收到 typea 广播
	c3.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {
		t.Errorf("typeb should not receive typea fanout")
	})

	packet := kkcluster.NewClusterPacket()
	packet.FuncName = "fanout"
	packet.ArgBytes = []byte("all-a")

	if err := c1.PublishRemoteType("typea", packet); err != nil {
		t.Fatalf("PublishRemoteType: %v", err)
	}

	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	n := gotTypeA
	mu.Unlock()
	if n != 1 {
		t.Fatalf("typea handlers want 1 (node2 only), got %d", n)
	}
}

func TestNoDiscovery_StartStop_IdempotentAndRestart(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	opt := kkcluster.ApplyOptions(kkcluster.WithUrl(natsURL))
	c1 := NewNatsCluster("node1", "typea", opt)
	c2 := NewNatsCluster("node2", "typea", opt)

	if err := c1.Start(); err != nil {
		t.Fatalf("c1 first Start: %v", err)
	}
	if err := c2.Start(); err != nil {
		c1.Stop()
		t.Fatalf("c2 first Start: %v", err)
	}

	// second Start should be idempotent no-op
	if err := c1.Start(); err != nil {
		t.Fatalf("c1 second Start: %v", err)
	}
	if err := c2.Start(); err != nil {
		t.Fatalf("c2 second Start: %v", err)
	}

	// second Stop should be idempotent no-op
	c1.Stop()
	c1.Stop()
	c2.Stop()
	c2.Stop()

	// restart after Stop should work
	if err := c1.Start(); err != nil {
		t.Fatalf("c1 restart Start: %v", err)
	}
	if err := c2.Start(); err != nil {
		c1.Stop()
		t.Fatalf("c2 restart Start: %v", err)
	}
	defer func() {
		c1.Stop()
		c2.Stop()
	}()

	time.Sleep(200 * time.Millisecond)

	received := make(chan string, 1)
	c2.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {
		received <- nodeID + "|" + string(packet.ArgBytes)
	})

	packet := kkcluster.NewClusterPacket()
	packet.FuncName = "restart-ping"
	packet.ArgBytes = []byte("ok")
	if err := c1.PublishRemote("node2", packet); err != nil {
		t.Fatalf("PublishRemote after restart: %v", err)
	}

	select {
	case got := <-received:
		if got != "node1|ok" {
			t.Fatalf("handler got %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting publish after restart")
	}
}
