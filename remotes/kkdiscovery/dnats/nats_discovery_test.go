package dnats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
)

// TestNatsDiscovery_New 测试创建NatsDiscovery
func TestNatsDiscovery_New(t *testing.T) {
	nodeInfo := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	discovery := NewNatsDiscovery("test", nodeInfo, nil, ApplyNatsOptions())

	if discovery == nil {
		t.Fatal("NewNatsDiscovery returned nil")
	}

	if discovery.Name() != "test" {
		t.Errorf("Name() = %s, want test", discovery.Name())
	}

	members := discovery.MemberCount()
	if members != 0 {
		t.Errorf("MemberCount() = %d, want 0", members)
	}
}

// TestNatsDiscovery_StartStop 测试启动和停止
func TestNatsDiscovery_StartStop(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	nodeInfo := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	discovery := NewNatsDiscovery("test", nodeInfo, nil, ApplyNatsOptions(WithUrl(natsURL)))

	if err := discovery.Start(); err != nil {
		t.Fatalf("Start() failed: %v", err)
	}

	// 等待一下让连接建立
	time.Sleep(200 * time.Millisecond)

	// 停止
	discovery.Stop()

	// 再次停止应该不会panic
	discovery.Stop()
}

// TestNatsDiscovery_Discovery 测试服务发现
func TestNatsDiscovery_Discovery(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	// 创建两个节点
	nodeInfo1 := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	discovery1 := NewNatsDiscovery("test1", nodeInfo1, nil, ApplyNatsOptions(WithUrl(natsURL)))
	nodeInfo2 := kkapp.NewNodeInfo("node2", "type1", "127.0.0.1:8081", "", nil)
	discovery2 := NewNatsDiscovery("test2", nodeInfo2, nil, ApplyNatsOptions(WithUrl(natsURL)))

	// 启动两个节点
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

	if !waitForMembers(discovery2, 1, 3*time.Second) {
		t.Fatal("discovery2 did not discover node1")
	}

	// 验证成员信息
	member, found := discovery1.GetMember("node2")
	if !found {
		t.Fatal("discovery1.GetMember(node2) not found")
	}

	if member.GetNodeType() != "type1" {
		t.Errorf("member.GetNodeType() = %s, want type1", member.GetNodeType())
	}

	if member.GetAddress() != "127.0.0.1:8081" {
		t.Errorf("member.GetAddress() = %s, want 127.0.0.1:8081", member.GetAddress())
	}
}

// TestNatsDiscovery_ListByType 测试按类型列出成员
func TestNatsDiscovery_ListByType(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	nodeInfo := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	discovery := NewNatsDiscovery("test", nodeInfo, nil, ApplyNatsOptions(WithUrl(natsURL)))

	// 手动添加成员
	member1 := kkdiscovery.NewMember("node2", "type1", "127.0.0.1:8081", 0, kkdiscovery.NodeStatusOnline, nil)
	member2 := kkdiscovery.NewMember("node3", "type2", "127.0.0.1:8082", 0, kkdiscovery.NodeStatusOnline, nil)
	member3 := kkdiscovery.NewMember("node4", "type1", "127.0.0.1:8083", 0, kkdiscovery.NodeStatusOnline, nil)

	discovery.(*NatsDiscovery).addMember(member1)
	discovery.(*NatsDiscovery).addMember(member2)
	discovery.(*NatsDiscovery).addMember(member3)

	// 测试按类型列出
	type1Members := discovery.ListByType("type1")
	if len(type1Members) != 2 {
		t.Errorf("ListByType(type1) length = %d, want 2", len(type1Members))
	}

	type2Members := discovery.ListByType("type2")
	if len(type2Members) != 1 {
		t.Errorf("ListByType(type2) length = %d, want 1", len(type2Members))
	}

	// 测试过滤
	filtered := discovery.ListByType("type1", "node2")
	if len(filtered) != 1 {
		t.Errorf("ListByType(type1, node2) length = %d, want 1", len(filtered))
	}
	if filtered[0].GetNodeID() != "node4" {
		t.Errorf("filtered member ID = %s, want node4", filtered[0].GetNodeID())
	}
}

// TestNatsDiscovery_Random 测试随机获取成员
func TestNatsDiscovery_Random(t *testing.T) {
	nodeInfo := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	discovery := NewNatsDiscovery("test", nodeInfo, nil, ApplyNatsOptions())

	// 空列表
	member, found := discovery.Random("type1")
	if found {
		t.Error("Random() should return false for empty list")
	}
	if member != nil {
		t.Error("Random() should return nil for empty list")
	}

	// 添加成员
	member1 := kkdiscovery.NewMember("node2", "type1", "127.0.0.1:8081", 0, kkdiscovery.NodeStatusOnline, nil)
	discovery.(*NatsDiscovery).addMember(member1)

	member, found = discovery.Random("type1")
	if !found {
		t.Error("Random() should return true")
	}
	if member == nil {
		t.Error("Random() should return member")
	}
	if member.GetNodeID() != "node2" {
		t.Errorf("Random() returned node ID = %s, want node2", member.GetNodeID())
	}
}

// TestNatsDiscovery_GetType 测试获取节点类型
func TestNatsDiscovery_GetType(t *testing.T) {
	nodeInfo := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	discovery := NewNatsDiscovery("test", nodeInfo, nil, ApplyNatsOptions())

	member := kkdiscovery.NewMember("node2", "type1", "127.0.0.1:8081", 0, kkdiscovery.NodeStatusOnline, nil)
	discovery.(*NatsDiscovery).addMember(member)

	nodeType, err := discovery.GetType("node2")
	if err != nil {
		t.Fatalf("GetType() failed: %v", err)
	}
	if nodeType != "type1" {
		t.Errorf("GetType() = %s, want type1", nodeType)
	}

	// 不存在的节点
	_, err = discovery.GetType("nonexistent")
	if err == nil {
		t.Error("GetType() should return error for nonexistent node")
	}
}

// TestNatsDiscovery_AddRemoveMember 测试添加和移除成员
func TestNatsDiscovery_AddRemoveMember(t *testing.T) {
	nodeInfo := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	discovery := NewNatsDiscovery("test", nodeInfo, nil, ApplyNatsOptions())

	member := kkdiscovery.NewMember("node2", "type1", "127.0.0.1:8081", 0, kkdiscovery.NodeStatusOnline, nil)
	discovery.(*NatsDiscovery).addMember(member)

	if discovery.MemberCount() != 1 {
		t.Errorf("MemberCount() = %d, want 1", discovery.MemberCount())
	}

	discovery.(*NatsDiscovery).removeMember("node2")

	if discovery.MemberCount() != 0 {
		t.Errorf("MemberCount() = %d, want 0", discovery.MemberCount())
	}
}

// TestNatsDiscovery_Listeners 测试监听器
func TestNatsDiscovery_Listeners(t *testing.T) {
	nodeInfo := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	discovery := NewNatsDiscovery("test", nodeInfo, nil, ApplyNatsOptions())

	addCalled := false
	removeCalled := false

	discovery.OnAddMember(func(member kkdiscovery.IMember) {
		addCalled = true
		if member.GetNodeID() != "node2" {
			t.Errorf("OnAddMember received node ID = %s, want node2", member.GetNodeID())
		}
	})

	discovery.OnRemoveMember(func(member kkdiscovery.IMember) {
		removeCalled = true
		if member.GetNodeID() != "node2" {
			t.Errorf("OnRemoveMember received node ID = %s, want node2", member.GetNodeID())
		}
	})

	member := kkdiscovery.NewMember("node2", "type1", "127.0.0.1:8081", 0, kkdiscovery.NodeStatusOnline, nil)
	discovery.(*NatsDiscovery).addMember(member)

	if !addCalled {
		t.Error("OnAddMember listener was not called")
	}

	discovery.(*NatsDiscovery).removeMember("node2")

	if !removeCalled {
		t.Error("OnRemoveMember listener was not called")
	}
}

// TestNatsDiscovery_MemberTimeout 测试成员超时
func TestNatsDiscovery_MemberTimeout(t *testing.T) {
	t.Skip("Skipping timeout test as it requires long wait time")

	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Skipf("NATS not available: %v", err)
	}

	nodeInfo1 := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	discovery1 := NewNatsDiscovery("test1", nodeInfo1, nil, ApplyNatsOptions(WithUrl(natsURL)))
	nodeInfo2 := kkapp.NewNodeInfo("node2", "type1", "127.0.0.1:8081", "", nil)
	discovery2 := NewNatsDiscovery("test2", nodeInfo2, nil, ApplyNatsOptions(WithUrl(natsURL)))

	if err := discovery1.Start(); err != nil {
		t.Fatalf("discovery1.Start() failed: %v", err)
	}
	defer discovery1.Stop()

	if err := discovery2.Start(); err != nil {
		t.Fatalf("discovery2.Start() failed: %v", err)
	}

	// 等待发现
	if !waitForMembers(discovery1, 1, 3*time.Second) {
		t.Fatal("discovery1 did not discover node2")
	}

	// 停止discovery2
	discovery2.Stop()

	// 等待超时（注意：实际超时时间是15秒，这里我们等待足够长的时间）
	// 由于测试时间较长，我们可以缩短超时时间或使用更短的检查间隔
	time.Sleep(20 * time.Second)

	// 验证node2被移除
	if discovery1.MemberCount() != 0 {
		t.Errorf("discovery1 should have removed node2, but MemberCount() = %d", discovery1.MemberCount())
	}
}

// startTestNatsServer 返回测试用的NATS服务器地址
// 注意：假设NATS服务器已经在本地运行（默认地址：nats://127.0.0.1:4222）
// 如果NATS服务器运行在其他地址，可以修改defaultNatsAddress或传入自定义地址
func startTestNatsServer() (interface{}, string, error) {
	// 使用默认地址；如果本地没有运行 NATS，则返回 error（测试会 Skip）
	url := "nats://127.0.0.1:4222"
	nc, err := nats.Connect(url, nats.Timeout(500*time.Millisecond))
	if err != nil {
		return nil, url, err
	}
	nc.Close()
	return nil, url, nil
}

// waitForMembers 等待成员出现
func waitForMembers(d kkdiscovery.IDiscovery, count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d.MemberCount() >= count {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
