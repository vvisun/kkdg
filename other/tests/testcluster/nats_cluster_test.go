package testcluster

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
)

// TestNatsCluster_New 测试创建NatsCluster
func TestNatsCluster_New(t *testing.T) {
	nodeInfo := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	discovery := dnats.NewNatsDiscovery("test", nodeInfo, dnats.ApplyNatsOptions(), kkdiscovery.ApplyOptions())
	cluster := cnats.NewNatsCluster("node1", "typea", discovery, cnats.ApplyNatsOptions(), kkcluster.ApplyOptions())

	if cluster == nil {
		t.Fatal("NewNatsCluster returned nil")
	}
}

// TestNatsCluster_Init 测试初始化
func TestNatsCluster_Init(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	nodeInfo := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	discovery := dnats.NewNatsDiscovery("test", nodeInfo, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	cluster := cnats.NewNatsCluster("node1", "typea", discovery, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())

	if err := cluster.Start(); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	// 等待连接建立
	time.Sleep(100 * time.Millisecond)

	cluster.Stop()
}

// TestNatsCluster_PublishRemote 测试发布消息到指定节点
func TestNatsCluster_PublishRemote(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	// 创建两个节点
	nodeInfo1 := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	discovery1 := dnats.NewNatsDiscovery("test1", nodeInfo1, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	nodeInfo2 := kkapp.NewNodeInfo("node2", "typea", "127.0.0.1:8081", "", nil)
	discovery2 := dnats.NewNatsDiscovery("test2", nodeInfo2, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())

	if err := discovery1.Start(); err != nil {
		t.Fatalf("discovery1.Start() failed: %v", err)
	}
	defer discovery1.Stop()

	if err := discovery2.Start(); err != nil {
		t.Fatalf("discovery2.Start() failed: %v", err)
	}
	defer discovery2.Stop()

	// 等待发现
	if !waitForMembers(discovery1, 1, 3*time.Second) {
		t.Fatal("discovery1 did not discover node2")
	}

	cluster1 := cnats.NewNatsCluster("node1", "typea", discovery1, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())
	cluster2 := cnats.NewNatsCluster("node2", "typea", discovery2, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())

	if err := cluster1.Start(); err != nil {
		t.Fatalf("cluster1.Init() failed: %v", err)
	}
	defer cluster1.Stop()

	if err := cluster2.Start(); err != nil {
		t.Fatalf("cluster2.Init() failed: %v", err)
	}
	defer cluster2.Stop()

	// 等待连接建立
	time.Sleep(200 * time.Millisecond)

	// 设置接收处理器
	received := make(chan *kkcluster.ClusterPacket, 1)
	cluster2.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {
		received <- packet
	})

	// 发布消息
	packet := &kkcluster.ClusterPacket{
		FuncName: "test",
		ArgBytes: []byte("hello"),
	}

	if err := cluster1.PublishRemote("node2", packet); err != nil {
		t.Fatalf("PublishRemote() failed: %v", err)
	}

	// 等待接收
	select {
	case p := <-received:
		if p.FuncName != "test" {
			t.Errorf("Received packet FuncName = %s, want test", p.FuncName)
		}
		if string(p.ArgBytes) != "hello" {
			t.Errorf("Received packet ArgBytes = %s, want hello", string(p.ArgBytes))
		}
		if p.SourcePath != "node1" {
			t.Errorf("Received packet SourcePath = %s, want node1", p.SourcePath)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Did not receive published message")
	}
}

// TestNatsCluster_PublishRemoteType 测试按类型发布消息
func TestNatsCluster_PublishRemoteType(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	// 创建三个节点，两个同类型
	nodeInfo1 := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	nodeInfo2 := kkapp.NewNodeInfo("node2", "typea", "127.0.0.1:8081", "", nil)
	nodeInfo3 := kkapp.NewNodeInfo("node3", "typeb", "127.0.0.1:8082", "", nil)
	discovery1 := dnats.NewNatsDiscovery("test1", nodeInfo1, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	discovery2 := dnats.NewNatsDiscovery("test2", nodeInfo2, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	discovery3 := dnats.NewNatsDiscovery("test3", nodeInfo3, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())

	if err := discovery1.Start(); err != nil {
		t.Fatalf("discovery1.Start() failed: %v", err)
	}
	defer discovery1.Stop()

	if err := discovery2.Start(); err != nil {
		t.Fatalf("discovery2.Start() failed: %v", err)
	}
	defer discovery2.Stop()

	if err := discovery3.Start(); err != nil {
		t.Fatalf("discovery3.Start() failed: %v", err)
	}
	defer discovery3.Stop()

	// 等待发现
	if !waitForMembers(discovery1, 2, 3*time.Second) {
		t.Fatal("discovery1 did not discover other nodes")
	}

	cluster1 := cnats.NewNatsCluster("node1", "typea", discovery1, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())
	cluster2 := cnats.NewNatsCluster("node2", "typea", discovery2, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())
	cluster3 := cnats.NewNatsCluster("node3", "typeb", discovery3, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())

	if err := cluster1.Start(); err != nil {
		t.Fatalf("cluster1.Init() failed: %v", err)
	}
	defer cluster1.Stop()

	if err := cluster2.Start(); err != nil {
		t.Fatalf("cluster2.Init() failed: %v", err)
	}
	defer cluster2.Stop()

	if err := cluster3.Start(); err != nil {
		t.Fatalf("cluster3.Init() failed: %v", err)
	}
	defer cluster3.Stop()

	// 等待连接建立
	time.Sleep(200 * time.Millisecond)

	// 设置接收处理器
	received2 := make(chan *kkcluster.ClusterPacket, 1)
	received3 := make(chan *kkcluster.ClusterPacket, 1)

	cluster2.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {
		received2 <- packet
	})

	cluster3.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {
		received3 <- packet
	})

	// 发布消息到typea类型
	packet := &kkcluster.ClusterPacket{
		FuncName: "test",
		ArgBytes: []byte("hello"),
	}

	if err := cluster1.PublishRemoteType("typea", packet); err != nil {
		t.Fatalf("PublishRemoteType() failed: %v", err)
	}

	// node2应该收到，node3不应该收到
	select {
	case p := <-received2:
		if p.FuncName != "test" {
			t.Errorf("node2 received packet FuncName = %s, want test", p.FuncName)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("node2 did not receive published message")
	}

	// node3不应该收到
	select {
	case <-received3:
		t.Error("node3 should not receive message for typea")
	case <-time.After(500 * time.Millisecond):
		// 这是期望的行为
	}
}

// TestNatsCluster_RequestRemote 测试请求-响应
func TestNatsCluster_RequestRemote(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	// 创建两个节点
	nodeInfo1 := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	nodeInfo2 := kkapp.NewNodeInfo("node2", "typea", "127.0.0.1:8081", "", nil)
	discovery1 := dnats.NewNatsDiscovery("test1", nodeInfo1, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	discovery2 := dnats.NewNatsDiscovery("test2", nodeInfo2, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())

	if err := discovery1.Start(); err != nil {
		t.Fatalf("discovery1.Start() failed: %v", err)
	}
	defer discovery1.Stop()

	if err := discovery2.Start(); err != nil {
		t.Fatalf("discovery2.Start() failed: %v", err)
	}
	defer discovery2.Stop()

	// 等待发现
	if !waitForMembers(discovery1, 1, 3*time.Second) {
		t.Fatal("discovery1 did not discover node2")
	}

	cluster1 := cnats.NewNatsCluster("node1", "typea", discovery1, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())
	cluster2 := cnats.NewNatsCluster("node2", "typea", discovery2, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())

	if err := cluster1.Start(); err != nil {
		t.Fatalf("cluster1.Init() failed: %v", err)
	}
	defer cluster1.Stop()

	if err := cluster2.Start(); err != nil {
		t.Fatalf("cluster2.Init() failed: %v", err)
	}
	defer cluster2.Stop()

	// 等待连接建立
	time.Sleep(200 * time.Millisecond)

	// 注意：RequestRemote需要设置请求处理器，但目前实现中handleRequest返回空响应
	// 这里主要测试请求不会panic
	packet := &kkcluster.ClusterPacket{
		FuncName: "test",
		ArgBytes: []byte("request"),
	}

	data, code := cluster1.RequestRemote("node2", packet, 2*time.Second)
	// 由于当前实现返回空响应，code应该是0，data应该是nil
	if code != kkcluster.ClusterErrorCodeFail {
		t.Errorf("RequestRemote() code = %d, want ClusterErrorCodeFail", code)
	}
	_ = data // 当前实现返回nil
}

// TestNatsCluster_PublishRemote_NotFound 测试发布到不存在的节点
func TestNatsCluster_PublishRemote_NotFound(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	nodeInfo := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	discovery := dnats.NewNatsDiscovery("test", nodeInfo, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	cluster := cnats.NewNatsCluster("node1", "typea", discovery, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())

	if err := cluster.Start(); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}
	defer cluster.Stop()

	packet := &kkcluster.ClusterPacket{
		FuncName: "test",
		ArgBytes: []byte("hello"),
	}

	err = cluster.PublishRemote("nonexistent", packet)
	if err == nil {
		t.Error("PublishRemote() should return error for nonexistent node")
	}
	if err != kkerrors.ErrClusterMemberNotFound {
		t.Errorf("PublishRemote() error = %v, want ErrMemberNotFound", err)
	}
}

// TestNatsCluster_PublishRemoteType_NoMember 测试发布到没有成员的类型
func TestNatsCluster_PublishRemoteType_NoMember(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	nodeInfo := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	discovery := dnats.NewNatsDiscovery("test", nodeInfo, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	cluster := cnats.NewNatsCluster("node1", "typea", discovery, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())

	if err := cluster.Start(); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}
	defer cluster.Stop()

	packet := &kkcluster.ClusterPacket{
		FuncName: "test",
		ArgBytes: []byte("hello"),
	}

	err = cluster.PublishRemoteType("nonexistent", packet)
	if err == nil {
		t.Error("PublishRemoteType() should return error for nonexistent type")
	}
	if err != kkerrors.ErrClusterNoMemberOfType {
		t.Errorf("PublishRemoteType() error = %v, want ErrNoMemberOfType", err)
	}
}

// TestNatsCluster_RequestRemoteAsync 测试异步请求
func TestNatsCluster_RequestRemoteAsync(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	nodeInfo1 := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	nodeInfo2 := kkapp.NewNodeInfo("node2", "typea", "127.0.0.1:8081", "", nil)
	discovery1 := dnats.NewNatsDiscovery("test1", nodeInfo1, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	discovery2 := dnats.NewNatsDiscovery("test2", nodeInfo2, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())

	if err := discovery1.Start(); err != nil {
		t.Fatalf("discovery1.Start() failed: %v", err)
	}
	defer discovery1.Stop()

	if err := discovery2.Start(); err != nil {
		t.Fatalf("discovery2.Start() failed: %v", err)
	}
	defer discovery2.Stop()

	if !waitForMembers(discovery1, 1, 3*time.Second) {
		t.Fatal("discovery1 did not discover node2")
	}

	cluster1 := cnats.NewNatsCluster("node1", "typea", discovery1, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())
	cluster2 := cnats.NewNatsCluster("node2", "typea", discovery2, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())

	if err := cluster1.Start(); err != nil {
		t.Fatalf("cluster1.Init() failed: %v", err)
	}
	defer cluster1.Stop()

	if err := cluster2.Start(); err != nil {
		t.Fatalf("cluster2.Init() failed: %v", err)
	}
	defer cluster2.Stop()

	time.Sleep(200 * time.Millisecond)

	// 测试 nil callback 返回错误
	packet := &kkcluster.ClusterPacket{FuncName: "test", ArgBytes: []byte("req")}
	err = cluster1.RequestRemoteAsync("node2", packet, nil, 2*time.Second)
	if err == nil {
		t.Error("RequestRemoteAsync with nil callback should return error")
	}

	// 测试异步回调（与 RequestRemote 类似，无 handler 时收到 Fail 响应）
	packet = &kkcluster.ClusterPacket{FuncName: "test", ArgBytes: []byte("req")}
	done := make(chan struct{})
	var gotData []byte
	var gotCode kkcluster.ClusterErrorCode

	err = cluster1.RequestRemoteAsync("node2", packet, func(data []byte, code kkcluster.ClusterErrorCode) {
		gotData = data
		gotCode = code
		close(done)
	}, 2*time.Second)
	if err != nil {
		t.Fatalf("RequestRemoteAsync() failed: %v", err)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("RequestRemoteAsync callback timeout")
	}

	// 无 handler 时收到 Fail
	if gotCode != kkcluster.ClusterErrorCodeFail {
		t.Errorf("RequestRemoteAsync expected ClusterErrorCodeFail, got %v", gotCode)
	}
	_ = gotData
}

// TestNatsCluster_RequestRemoteAsync_NotFound 测试异步请求到不存在的节点
func TestNatsCluster_RequestRemoteAsync_NotFound(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	nodeInfo := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	discovery := dnats.NewNatsDiscovery("test", nodeInfo, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	cluster := cnats.NewNatsCluster("node1", "typea", discovery, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())

	if err := cluster.Start(); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}
	defer cluster.Stop()

	packet := &kkcluster.ClusterPacket{FuncName: "test", ArgBytes: []byte("hello")}
	err = cluster.RequestRemoteAsync("nonexistent", packet, func([]byte, kkcluster.ClusterErrorCode) {}, 2*time.Second)
	if err == nil {
		t.Error("RequestRemoteAsync should return error for nonexistent node")
	}
	if err != kkerrors.ErrClusterMemberNotFound {
		t.Errorf("RequestRemoteAsync error = %v, want ErrMemberNotFound", err)
	}
}

// TestNatsCluster_Stop 测试停止
func TestNatsCluster_Stop(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	nodeInfo := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	discovery := dnats.NewNatsDiscovery("test", nodeInfo, dnats.ApplyNatsOptions(dnats.WithUrl(natsURL)), kkdiscovery.ApplyOptions())
	cluster := cnats.NewNatsCluster("node1", "typea", discovery, cnats.ApplyNatsOptions(cnats.WithUrl(natsURL)), kkcluster.ApplyOptions())

	if err := cluster.Start(); err != nil {
		t.Fatalf("Init() failed: %v", err)
	}

	cluster.Stop()

	// 再次停止应该不会panic
	cluster.Stop()
}
