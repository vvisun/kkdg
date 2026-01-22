package testcluster

import (
	"testing"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kknet/kkdiscovery"
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

	if member.GetSettings() == nil {
		t.Error("GetSettings() returned nil")
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

	gotSettings := member.GetSettings()
	if len(gotSettings) != 2 {
		t.Errorf("GetSettings() length = %d, want 2", len(gotSettings))
	}

	if gotSettings["key1"] != "value1" {
		t.Errorf("GetSettings()[key1] = %s, want value1", gotSettings["key1"])
	}

	if gotSettings["key2"] != "value2" {
		t.Errorf("GetSettings()[key2] = %s, want value2", gotSettings["key2"])
	}
}

// TestMember_ImplementsInterface 测试Member实现IMember接口
func TestMember_ImplementsInterface(t *testing.T) {
	var _ kkdiscovery.IMember = (*kkdiscovery.Member)(nil)
}
