package testcluster

import (
	"testing"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
)

// TestMember_NewMember 测试创建成员
func TestMember_NewMember(t *testing.T) {
	nodeInfo := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	member := kkdiscovery.NewMember(nodeInfo.GetNodeId(), nodeInfo.GetNodeType(), nodeInfo.GetAddress(), nil)

	if member == nil {
		t.Fatal("NewMember returned nil")
	}

	if member.GetNodeID() != "node1" {
		t.Errorf("GetNodeID() = %s, want node1", member.GetNodeID())
	}

	if member.GetNodeType() != "type1" {
		t.Errorf("GetNodeType() = %s, want type1", member.GetNodeType())
	}

	if member.GetAddress() != "127.0.0.1:8080" {
		t.Errorf("GetAddress() = %s, want 127.0.0.1:8080", member.GetAddress())
	}
}

// TestMember_WithSettings 测试带设置的成员
func TestMember_WithSettings(t *testing.T) {
	settings := map[string]string{
		"key1": "value1",
		"key2": "value2",
	}

	nodeInfo := kkapp.NewNodeInfo("node1", "type1", "127.0.0.1:8080", "", nil)
	member := kkdiscovery.NewMember(nodeInfo.GetNodeId(), nodeInfo.GetNodeType(), nodeInfo.GetAddress(), settings)

	if member == nil {
		t.Fatal("NewMember returned nil")
	}

	if value, ok := member.GetSetting("key1"); !ok || value != "value1" {
		t.Errorf("GetSetting(key1) = %s, want value1", value)
	}

	if value, ok := member.GetSetting("key2"); !ok || value != "value2" {
		t.Errorf("GetSetting(key2) = %s, want value2", value)
	}
}

// TestMember_ImplementsInterface 测试Member实现IMember接口
func TestMember_ImplementsInterface(t *testing.T) {
	var _ kkdiscovery.IMember = (*kkdiscovery.Member)(nil)
}
