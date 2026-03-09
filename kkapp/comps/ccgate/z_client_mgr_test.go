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

func Test_clientInfo_getLogicNode_allocLogicNode_removeLogicNode(t *testing.T) {
	ci := newClientInfo(1, "s1")
	if ci.getLogicNode("game") != nil {
		t.Error("getLogicNode on empty should return nil")
	}

	lgcNode := ci.allocLogicNode("game", "node1")
	if lgcNode == nil {
		t.Error("allocLogicNode should return non-nil")
	}
	if lgcNode.nodeId != "node1" || lgcNode.nodeType != "game" {
		t.Errorf("allocLogicNode result nodeId=%v nodeType=%v", lgcNode.nodeId, lgcNode.nodeType)
	}

	ci.removeLogicNode("game")
	if ci.getLogicNode("game") != nil {
		t.Error("getLogicNode after removeLogicNode should return nil")
	}

	ci.allocLogicNode("game", "node2")
	lgcNode = ci.getLogicNode("game")
	if lgcNode.nodeId != "node2" || lgcNode.nodeType != "game" {
		t.Errorf("allocLogicNode result nodeId=%v nodeType=%v", lgcNode.nodeId, lgcNode.nodeType)
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
	m := &clientManager{}

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

	got := m.getClient(connID)
	if got != ci {
		t.Errorf("getClient(%v) = %v, want %v", connID, got, ci)
	}

	// get non-existent
	if m.getClient(999) != nil {
		t.Error("getClient(999) should return nil")
	}

	// remove
	m.removeClient(connID)
	if m.getClient(connID) != nil {
		t.Error("getClient after removeClient should return nil")
	}

	// remove non-existent should not panic
	m.removeClient(999)
}

func Test_clientManager_getClientByUserId_loginToGate(t *testing.T) {
	m := &clientManager{}
	connID := kknet.CONN_ID(200)
	m.addClient(connID, getSessionId(connID, "gate1"))

	if m.getClientByUserId(100) != nil {
		t.Error("getClientByUserId before login should return nil")
	}

	// login with NULL_USER_ID should fail
	if _, kickConnId := m.loginToGate(connID, kknet.NULL_USER_ID); kickConnId != kknet.NULL_CONN_ID {
		t.Error("loginToGate with NULL_USER_ID should return false")
	}

	// login with valid userId
	if _, kickConnId := m.loginToGate(connID, 100); kickConnId != kknet.NULL_CONN_ID {
		t.Error("loginToGate(200, 100) should return true")
	}
	ci := m.getClientByUserId(100)
	if ci == nil || ci.userId != 100 {
		t.Errorf("getClientByUserId(100) = %v, want clientInfo with userId 100", ci)
	}

	// login on non-existent conn should fail
	if _, kickConnId := m.loginToGate(999, 101); kickConnId != kknet.NULL_CONN_ID {
		t.Error("loginToGate on non-existent conn should return false")
	}
}

func Test_clientManager_allocLogicNode(t *testing.T) {
	m := &clientManager{}
	connID := kknet.CONN_ID(300)
	m.addClient(connID, getSessionId(connID, "gate1"))

	// alloc on existing client
	info1 := m.allocLogicNode(connID, "game", "game1")
	if info1 == nil {
		t.Fatal("allocLogicNode should return non-nil")
	}
	if info1.nodeId != "game1" || info1.nodeType != "game" {
		t.Errorf("allocLogicNode result nodeId=%v nodeType=%v", info1.nodeId, info1.nodeType)
	}

	// same nodeType returns same info
	info2 := m.allocLogicNode(connID, "game", "game2")
	if info2 != info1 {
		t.Error("allocLogicNode same nodeType should return existing logicNodeInfo")
	}

	// alloc on non-existent conn returns nil
	if m.allocLogicNode(999, "game", "game1") != nil {
		t.Error("allocLogicNode on non-existent conn should return nil")
	}
}

func Test_clientManager_loginToLogicNode(t *testing.T) {
	m := &clientManager{}
	connID := kknet.CONN_ID(400)
	m.addClient(connID, getSessionId(connID, "gate1"))
	m.allocLogicNode(connID, "game", "game1")

	if m.loginToLogicNode(connID, "game", kknet.NULL_USER_ID) {
		t.Error("loginToLogicNode with NULL_USER_ID should return false")
	}
	if m.loginToLogicNode(999, "game", 100) {
		t.Error("loginToLogicNode on non-existent conn should return false")
	}
	if m.loginToLogicNode(connID, "unknown", 100) {
		t.Error("loginToLogicNode with unallocated nodeType should return false")
	}

	if !m.loginToLogicNode(connID, "game", 100) {
		t.Error("loginToLogicNode(400, game, 100) should return true")
	}
	ci := m.getClient(connID)
	lgc := ci.getLogicNode("game")
	if lgc == nil || !lgc.isLogin() || lgc.userId != 100 {
		t.Errorf("logicNode after loginToLogicNode: %+v", lgc)
	}
}

func Test_clientManager_removeClient_clearsUserMap(t *testing.T) {
	m := &clientManager{}
	connID := kknet.CONN_ID(500)
	m.addClient(connID, getSessionId(connID, "gate1"))
	m.loginToGate(connID, 200)

	m.removeClient(connID)
	if m.getClientByUserId(200) != nil {
		t.Error("getClientByUserId after removeClient should return nil")
	}
}
