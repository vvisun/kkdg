package gametransnats

import (
	"bytes"
	"testing"

	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/remotes/kkcluster"
)

type stubReceiver struct {
	sids    []string
	payload [][]byte
}

func (s *stubReceiver) OnSession(sessionID string, packet []byte, threadIdx int) {
	s.sids = append(s.sids, sessionID)
	s.payload = append(s.payload, append([]byte(nil), packet...))
}

func newTestTransportor(recv gametrans.ISessionMsgReceiver) *transportorNats {
	return &transportorNats{
		sessionMgr:  gametrans.NewSessionManager(4),
		msgReceiver: recv,
	}
}

func TestOnPublish_CliMiss_EmptyPayloadRemovesSession(t *testing.T) {
	recv := &stubReceiver{}
	trans := newTestTransportor(recv)
	trans.sessionMgr.AddSession("gate1-9", "gate1")

	trans.onPublish("gate1", &kkcluster.ClusterPacket{
		FuncName: ptotrans.FuncNameClientDisconnect,
		Sid:      "gate1-9",
		ArgBytes: nil,
	})

	if trans.sessionMgr.GetSession("gate1-9") != nil {
		t.Fatal("cliMiss with empty payload must RemoveSession")
	}
	if trans.sessionMgr.OnlineCount() != 0 {
		t.Fatalf("OnlineCount = %d, want 0", trans.sessionMgr.OnlineCount())
	}
	if len(recv.sids) != 0 {
		t.Fatalf("cliMiss must not call OnSession, got %v", recv.sids)
	}
}

func TestOnPublish_C2S_DeliversPayload(t *testing.T) {
	recv := &stubReceiver{}
	trans := newTestTransportor(recv)
	body := []byte{0, 0, 0, 1, 99}

	trans.onPublish("gate1", &kkcluster.ClusterPacket{
		FuncName: ptotrans.FuncNameC2S,
		Sid:      "gate1-1",
		ArgBytes: body,
	})

	si := trans.sessionMgr.GetSession("gate1-1")
	if si == nil {
		t.Fatal("C2S must create session")
	}
	if si.GetGateNodeID() != "gate1" {
		t.Fatalf("gateNodeID = %q, want gate1", si.GetGateNodeID())
	}
	if len(recv.sids) != 1 || recv.sids[0] != "gate1-1" {
		t.Fatalf("OnSession sids = %v, want [gate1-1]", recv.sids)
	}
	if !bytes.Equal(recv.payload[0], body) {
		t.Fatalf("OnSession payload = %v, want %v", recv.payload[0], body)
	}
}

func TestOnPublish_C2S_EmptyPayloadIgnored(t *testing.T) {
	recv := &stubReceiver{}
	trans := newTestTransportor(recv)

	trans.onPublish("gate1", &kkcluster.ClusterPacket{
		FuncName: ptotrans.FuncNameC2S,
		Sid:      "gate1-1",
		ArgBytes: nil,
	})

	if trans.sessionMgr.GetSession("gate1-1") != nil {
		t.Fatal("empty C2S must not create session")
	}
	if len(recv.sids) != 0 {
		t.Fatalf("empty C2S must not call OnSession, got %v", recv.sids)
	}
}

func TestOnPublish_AllocClient_CreatesSessionWithoutOnSession(t *testing.T) {
	recv := &stubReceiver{}
	trans := newTestTransportor(recv)

	trans.onPublish("gate1", &kkcluster.ClusterPacket{
		FuncName: ptotrans.FuncNameAllocClient,
		Sid:      "gate1-2",
		ArgBytes: nil,
	})

	if trans.sessionMgr.GetSession("gate1-2") == nil {
		t.Fatal("allocClient must AddSession")
	}
	if len(recv.sids) != 0 {
		t.Fatalf("allocClient must not call OnSession, got %v", recv.sids)
	}
}

func TestOnPublish_UnknownFunc_Ignored(t *testing.T) {
	recv := &stubReceiver{}
	trans := newTestTransportor(recv)

	trans.onPublish("gate1", &kkcluster.ClusterPacket{
		FuncName: ptotrans.FuncNameSendToClient,
		Sid:      "gate1-3",
		ArgBytes: []byte("x"),
	})

	if trans.sessionMgr.GetSession("gate1-3") != nil {
		t.Fatal("unknown func must not create session")
	}
	if len(recv.sids) != 0 {
		t.Fatalf("unknown func must not call OnSession, got %v", recv.sids)
	}
}
