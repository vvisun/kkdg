package extmsg

import (
	"encoding/json"
	"testing"
)

func TestCmdConstants(t *testing.T) {
	cmds := []string{
		CmdClientDisconnect, CmdHeartbeat, CmdHeartbeatAck,
		CmdRegister, CmdUpdate,
	}
	for _, c := range cmds {
		if c == "" {
			t.Errorf("cmd constant is empty")
		}
	}
}

func TestUpMsg_JSONRoundtrip(t *testing.T) {
	orig := UpMsg{
		ConnID: 1001,
		Uid:    2002,
		Data:   json.RawMessage(`{"key":"value"}`),
		Cmd:    "login",
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got UpMsg
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ConnID != orig.ConnID || got.Uid != orig.Uid || got.Cmd != orig.Cmd {
		t.Errorf("got %+v, want %+v", got, orig)
	}
	if string(got.Data) != string(orig.Data) {
		t.Errorf("Data: got %s, want %s", got.Data, orig.Data)
	}
}

func TestDownMsg_JSONRoundtrip(t *testing.T) {
	orig := DownMsg{
		Cmd:    CmdRegister,
		ConnID: 1001,
		Data:   json.RawMessage(`{"shardIdx":0,"nodeId":"n1"}`),
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got DownMsg
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Cmd != orig.Cmd || got.ConnID != orig.ConnID || string(got.Data) != string(orig.Data) {
		t.Errorf("got %+v, want %+v", got, orig)
	}
}

func TestRegisterMsg_JSONRoundtrip(t *testing.T) {
	orig := RegisterMsg{
		ShardIdx: 3,
		NodeId:   "logic_1",
		NodeType: "logic",
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RegisterMsg
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.ShardIdx != orig.ShardIdx || got.NodeId != orig.NodeId || got.NodeType != orig.NodeType {
		t.Errorf("got %+v, want %+v", got, orig)
	}
}
