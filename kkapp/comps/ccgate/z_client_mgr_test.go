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

func Test_logicNodeInfo_isLogin_login(t *testing.T) {
	info := newClientLogicItem("node1", "game")
	if info.isLogin() {
		t.Error("new logicNodeInfo should not be login")
	}
	info.userId = 100
	if !info.isLogin() {
		t.Error("after login should be login")
	}
	if info.userId != 100 {
		t.Errorf("userId = %v, want 100", info.userId)
	}
}

func Test_clientManager_addClient_removeClient_getClient(t *testing.T) {
	m := newClientManager()

	// add and get
	connID := kknet.CONN_ID(100)
	sessionID := getSessionId(connID, "gate1")
	ci := m.addClient(connID, sessionID)
	if ci == nil {
		t.Fatal("addClient should return non-nil clientInfo")
	}
	if ci.connId != connID || ci.sessionId != sessionID {
		t.Errorf("clientInfo connId=%v sessionId=%v, want %v %v", ci.connId, ci.sessionId, connID, sessionID)
	}

	got := m.getClientByConnId(connID)
	if got != ci {
		t.Errorf("getClient(%v) = %v, want %v", connID, got, ci)
	}

	// get non-existent
	if m.getClientByConnId(999) != nil {
		t.Error("getClient(999) should return nil")
	}

	// remove
	m.removeClient(connID)
	if m.getClientByConnId(connID) != nil {
		t.Error("getClient after removeClient should return nil")
	}

	// remove non-existent should not panic
	m.removeClient(999)
}
