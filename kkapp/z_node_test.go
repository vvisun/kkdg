package kkapp

import (
	"testing"
)

func TestNewNodeInfo_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewNodeInfo with empty nodeId should panic")
		}
	}()
	NewNodeInfo("", "type", "127.0.0.1:8080", "")
}

func TestNewNodeInfo_PanicInvalidType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewNodeInfo with empty nodeType should panic")
		}
	}()
	NewNodeInfo("node1", "", "127.0.0.1:8080", "")
}

func TestNodeInfo_Getters(t *testing.T) {
	info := NewNodeInfo("node1", "gate", "127.0.0.1:8080", "127.0.0.1:8081")

	if info.GetNodeId() != "node1" {
		t.Errorf("GetNodeId() = %q, want node1", info.GetNodeId())
	}
	if info.GetNodeType() != "gate" {
		t.Errorf("GetNodeType() = %q, want gate", info.GetNodeType())
	}
	if info.GetAddress() != "127.0.0.1:8080" {
		t.Errorf("GetAddress() = %q, want 127.0.0.1:8080", info.GetAddress())
	}
	if info.GetRpcAddress() != "127.0.0.1:8081" {
		t.Errorf("GetRpcAddress() = %q, want 127.0.0.1:8081", info.GetRpcAddress())
	}
}
