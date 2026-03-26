package achecker

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
		{"empty", "", kkerrors.ErrAppInvalidNodeID},
		{"valid", "node1", nil},
		{"underscore", "node_1", nil},
		{"hyphen", "node-1", nil},
		{"long", "node123456789012345678", kkerrors.ErrAppInvalidNodeID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckNodeID(tt.nodeId)
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
		{"empty", "", kkerrors.ErrAppInvalidNodeType},
		{"valid", "gate", nil},
		{"letters_only", "game", nil},
		{"digit_suffix", "type1", kkerrors.ErrAppInvalidNodeType},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckNodeType(tt.nodeType)
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
