package cnats

import (
	"sync"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
)

// setupBenchCluster 创建两个节点的 discovery 和 cluster，供 benchmark 使用
// 需要 NATS 在 127.0.0.1:4222 运行，否则 b.Skip
func setupBenchCluster(b *testing.B) (cluster1, cluster2 kkcluster.ICluster, cleanup func()) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		b.Skipf("NATS not available: %v", err)
	}

	nodeInfo1 := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "")
	nodeInfo2 := kkapp.NewNodeInfo("node2", "typea", "127.0.0.1:8081", "")
	discovery1 := dnats.NewNatsDiscovery(nodeInfo1, kkdiscovery.ApplyOptions(
		kkdiscovery.WithUrl(natsURL),
	))
	discovery2 := dnats.NewNatsDiscovery(nodeInfo2, kkdiscovery.ApplyOptions(
		kkdiscovery.WithUrl(natsURL),
	))

	if err := discovery1.Start(); err != nil {
		b.Fatalf("discovery1.Start() failed: %v", err)
	}
	if err := discovery2.Start(); err != nil {
		discovery1.Stop()
		b.Fatalf("discovery2.Start() failed: %v", err)
	}

	if !waitForMembers(discovery1, 1, 3*time.Second) {
		discovery1.Stop()
		discovery2.Stop()
		b.Fatal("discovery1 did not discover node2")
	}

	cluster1 = NewNatsCluster("node1", "typea", discovery1, kkcluster.ApplyOptions(
		kkcluster.WithUrl(natsURL),
	))
	cluster2 = NewNatsCluster("node2", "typea", discovery2, kkcluster.ApplyOptions(
		kkcluster.WithUrl(natsURL),
	))

	if err := cluster1.Start(); err != nil {
		discovery1.Stop()
		discovery2.Stop()
		b.Fatalf("cluster1.Init() failed: %v", err)
	}
	if err := cluster2.Start(); err != nil {
		cluster1.Stop()
		discovery1.Stop()
		discovery2.Stop()
		b.Fatalf("cluster2.Init() failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	cleanup = func() {
		cluster1.Stop()
		cluster2.Stop()
		discovery1.Stop()
		discovery2.Stop()
	}
	return cluster1, cluster2, cleanup
}

// setupBenchClusterWithType 创建三个节点（两个 typea、一个 typeb），供 PublishRemoteType benchmark
func setupBenchClusterWithType(b *testing.B) (cluster1, cluster2, cluster3 kkcluster.ICluster, cleanup func()) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		b.Skipf("NATS not available: %v", err)
	}

	nodeInfo1 := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "")
	nodeInfo2 := kkapp.NewNodeInfo("node2", "typea", "127.0.0.1:8081", "")
	nodeInfo3 := kkapp.NewNodeInfo("node3", "typeb", "127.0.0.1:8082", "")
	discovery1 := dnats.NewNatsDiscovery(nodeInfo1, kkdiscovery.ApplyOptions(
		kkdiscovery.WithUrl(natsURL),
	))
	discovery2 := dnats.NewNatsDiscovery(nodeInfo2, kkdiscovery.ApplyOptions(
		kkdiscovery.WithUrl(natsURL),
	))
	discovery3 := dnats.NewNatsDiscovery(nodeInfo3, kkdiscovery.ApplyOptions(
		kkdiscovery.WithUrl(natsURL),
	))

	if err := discovery1.Start(); err != nil {
		b.Fatalf("discovery1.Start() failed: %v", err)
	}
	if err := discovery2.Start(); err != nil {
		b.Fatalf("discovery2.Start() failed: %v", err)
	}
	if err := discovery3.Start(); err != nil {
		b.Fatalf("discovery3.Start() failed: %v", err)
	}

	if !waitForMembers(discovery1, 2, 3*time.Second) {
		discovery1.Stop()
		discovery2.Stop()
		discovery3.Stop()
		b.Fatal("discovery1 did not discover other nodes")
	}

	cluster1 = NewNatsCluster("node1", "typea", discovery1, kkcluster.ApplyOptions(
		kkcluster.WithUrl(natsURL),
	))
	cluster2 = NewNatsCluster("node2", "typea", discovery2, kkcluster.ApplyOptions(
		kkcluster.WithUrl(natsURL),
	))
	cluster3 = NewNatsCluster("node3", "typeb", discovery3, kkcluster.ApplyOptions(
		kkcluster.WithUrl(natsURL),
	))

	if err := cluster1.Start(); err != nil {
		b.Fatalf("cluster1.Init() failed: %v", err)
	}
	if err := cluster2.Start(); err != nil {
		b.Fatalf("cluster2.Init() failed: %v", err)
	}
	if err := cluster3.Start(); err != nil {
		b.Fatalf("cluster3.Init() failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	cleanup = func() {
		cluster1.Stop()
		cluster2.Stop()
		cluster3.Stop()
		discovery1.Stop()
		discovery2.Stop()
		discovery3.Stop()
	}
	return cluster1, cluster2, cluster3, cleanup
}

func BenchmarkRequestRemote(b *testing.B) {
	cluster1, cluster2, cleanup := setupBenchCluster(b)
	defer func() { b.StopTimer(); cleanup() }()

	// 设置请求处理器，返回成功响应
	cluster2.SetRequestHandler(func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error) {
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeSuccess),
			Data:      []byte("ok"),
		}, nil
	})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		packet := kkcluster.NewClusterPacket()
		packet.FuncName = "test"
		packet.ArgBytes = []byte("bench")
		data, code := cluster1.RequestRemote("node2", packet, 5*time.Second)
		if code != kkcluster.ClusterErrorCodeSuccess {
			b.Fatalf("RequestRemote code = %v, want Success", code)
		}
		if string(data) != "ok" {
			b.Fatalf("RequestRemote data = %q, want ok", string(data))
		}
	}
}

// BenchmarkRequestRemoteAsync 测量异步请求往返。建议加 -benchtime=5s 获得更多迭代与稳定 ns/op。
func BenchmarkRequestRemoteAsync(b *testing.B) {
	cluster1, cluster2, cleanup := setupBenchCluster(b)
	defer func() { b.StopTimer(); cleanup() }()

	cluster2.SetRequestHandler(func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error) {
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeSuccess),
			Data:      []byte("ok"),
		}, nil
	})

	b.ResetTimer()
	b.ReportAllocs()
	var wg sync.WaitGroup
	for i := 0; i < b.N; i++ {
		packet := kkcluster.NewClusterPacket()
		packet.FuncName = "test"
		packet.ArgBytes = []byte("bench")
		wg.Add(1)
		err := cluster1.RequestRemoteAsync("node2", packet, func(data []byte, code kkcluster.ClusterErrorCode) {
			if code != kkcluster.ClusterErrorCodeSuccess {
				b.Errorf("RequestRemoteAsync code = %v", code)
			}
			wg.Done()
		}, 5*time.Second)
		if err != nil {
			wg.Done()
			b.Fatalf("RequestRemoteAsync: %v", err)
		}
		wg.Wait()
	}
}

func BenchmarkPublishRemote(b *testing.B) {
	cluster1, cluster2, cleanup := setupBenchCluster(b)
	defer func() { b.StopTimer(); cleanup() }()

	// 使用无操作 handler，避免接收端阻塞影响发送性能
	cluster2.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		packet := kkcluster.NewClusterPacket()
		packet.FuncName = "test"
		packet.ArgBytes = []byte("bench")
		if err := cluster1.PublishRemote("node2", packet); err != nil {
			b.Fatalf("PublishRemote: %v", err)
		}
	}
}

func BenchmarkPublishRemoteType(b *testing.B) {
	cluster1, cluster2, cluster3, cleanup := setupBenchClusterWithType(b)
	defer func() { b.StopTimer(); cleanup() }()

	cluster2.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {})
	cluster3.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {})

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		packet := kkcluster.NewClusterPacket()
		packet.FuncName = "test"
		packet.ArgBytes = []byte("bench")
		if err := cluster1.PublishRemoteType("typea", packet); err != nil {
			b.Fatalf("PublishRemoteType: %v", err)
		}
	}
}

// BenchmarkPublishRemoteParallel 并行 PublishRemote
func BenchmarkPublishRemoteParallel(b *testing.B) {
	cluster1, cluster2, cleanup := setupBenchCluster(b)
	defer func() { b.StopTimer(); cleanup() }()

	cluster2.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {})

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			packet := kkcluster.NewClusterPacket()
			packet.FuncName = "test"
			packet.ArgBytes = []byte("bench")
			_ = cluster1.PublishRemote("node2", packet)
		}
	})
}

// BenchmarkRequestRemoteParallel 并行 RequestRemote
func BenchmarkRequestRemoteParallel(b *testing.B) {
	cluster1, cluster2, cleanup := setupBenchCluster(b)
	defer func() { b.StopTimer(); cleanup() }()

	var mu sync.Mutex
	cluster2.SetRequestHandler(func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error) {
		mu.Lock()
		defer mu.Unlock()
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeSuccess),
			Data:      []byte("ok"),
		}, nil
	})

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			packet := kkcluster.NewClusterPacket()
			packet.FuncName = "test"
			packet.ArgBytes = []byte("bench")
			_, code := cluster1.RequestRemote("node2", packet, 5*time.Second)
			if code != kkcluster.ClusterErrorCodeSuccess {
				b.Errorf("RequestRemote code = %v", code)
			}
		}
	})
}
