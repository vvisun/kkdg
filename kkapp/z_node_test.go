package kkapp

import (
	"errors"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
)

func TestCheckNodeID(t *testing.T) {
	tests := []struct {
		name    string
		nodeId  string
		wantErr error
	}{
		{"empty", "", kkerrors.ErrInvalidNodeID},
		{"valid", "node1", nil},
		{"underscore", "node_1", kkerrors.ErrInvalidNodeID},
		{"long", "node123456789012345678", kkerrors.ErrInvalidNodeID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkNodeID(tt.nodeId)
			if tt.wantErr != nil {
				if err == nil || !errors.Is(err, tt.wantErr) {
					t.Errorf("CheckNodeID(%q) = %v, want %v", tt.nodeId, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("CheckNodeID(%q) = %v, want nil", tt.nodeId, err)
			}
		})
	}
}

func TestCheckNodeType(t *testing.T) {
	tests := []struct {
		name     string
		nodeType string
		wantErr  error
	}{
		{"empty", "", kkerrors.ErrInvalidNodeType},
		{"valid", "gate", nil},
		{"letters_only", "game", nil},
		{"digit_suffix", "type1", kkerrors.ErrInvalidNodeType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := checkNodeType(tt.nodeType)
			if tt.wantErr != nil {
				if err == nil || !errors.Is(err, tt.wantErr) {
					t.Errorf("CheckNodeType(%q) = %v, want %v", tt.nodeType, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("CheckNodeType(%q) = %v, want nil", tt.nodeType, err)
			}
		})
	}
}

func TestNewNodeInfo_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewNodeInfo with empty nodeId should panic")
		}
	}()
	NewNodeInfo("", "type", "127.0.0.1:8080", "", nil)
}

func TestNewNodeInfo_PanicInvalidType(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewNodeInfo with empty nodeType should panic")
		}
	}()
	NewNodeInfo("node1", "", "127.0.0.1:8080", "", nil)
}

func TestNodeInfo_Getters(t *testing.T) {
	settings := map[string]string{"k1": "v1", "k2": "v2"}
	info := NewNodeInfo("node1", "gate", "127.0.0.1:8080", "127.0.0.1:8081", settings)

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
	v, ok := info.GetSetting("k1")
	if !ok || v != "v1" {
		t.Errorf("GetSetting(k1) = %q, %v; want v1, true", v, ok)
	}
	_, ok = info.GetSetting("notexist")
	if ok {
		t.Error("GetSetting(notexist) should return false")
	}
}

func TestNodeInfo_GetSetting_NilSettings(t *testing.T) {
	// NewNodeInfo with nil settings - the struct field is nil
	info := NewNodeInfo("n", "t", "a", "r", nil)
	_, ok := info.GetSetting("any")
	if ok {
		t.Error("GetSetting on nil settings should return false")
	}
}
