package ccgate

import (
	"testing"

	"github.com/vvisun/kkdg/kknet"
)

func Test_getSessionId(t *testing.T) {
	tests := []struct {
		connID     kknet.CONN_ID
		gateNodeId string
		want       string
	}{
		{0, "gate1", "gate1-0"},
		{1, "gate1", "gate1-1"},
		{1, "gate2", "gate2-1"},
		{12345, "gate1", "gate1-12345"},
	}
	for _, tt := range tests {
		if got := getSessionId(tt.connID, tt.gateNodeId); got != tt.want {
			t.Errorf("getSessionId(%v) = %v, want %v", tt.connID, got, tt.want)
		}
	}
}
