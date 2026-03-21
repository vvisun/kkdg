package kkactor

import (
	"errors"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actortrans"
	"github.com/vvisun/kkdg/kkerrors"
)

func mustLucencyID(t *testing.T, nodeID, actorKey string) LucencyID {
	t.Helper()
	id, err := NewLucencyID(nodeID, actorKey)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

type stubRemoteTransport struct {
	startErr    error
	startCalls  int
	closeCalls  int
	receiverSet actortrans.IRemoteActorReceiver
	registrySet *actortrans.MessageRegistry
}

func (t *stubRemoteTransport) Start() error {
	t.startCalls++
	return t.startErr
}

func (t *stubRemoteTransport) Close() error {
	t.closeCalls++
	return nil
}

func (t *stubRemoteTransport) SetReceiver(receiver actortrans.IRemoteActorReceiver) {
	t.receiverSet = receiver
}

func (t *stubRemoteTransport) Send(target actortrans.ActorRef, msg any) error {
	return nil
}

func (t *stubRemoteTransport) Request(target actortrans.ActorRef, msg any, timeout time.Duration) (any, error) {
	return nil, nil
}

func (t *stubRemoteTransport) RequestAsync(target actortrans.ActorRef, msg any, timeout time.Duration, callback func(result any, err error)) error {
	if callback == nil {
		return kkerrors.ErrActorAsyncCallbackNil
	}
	callback(nil, nil)
	return nil
}

//------------------------------------------------------------------------------
// actor_id_test
//------------------------------------------------------------------------------

func TestNewLucencyActorID(t *testing.T) {
	tests := []struct {
		name     string
		nodeId   string
		actorKey string
		wantErr  error
	}{
		{"invalid_empty_node", "", "game_main", kkerrors.ErrActorInvalidNodeId},
		{"valid_with_node", "game1", "game_player", nil},
		{"valid_node_underscore", "node_1", "gate_router", nil},
		{"valid_node_hyphen", "node-1", "gate_router", nil},
		{"valid_actor_key_hyphen", "game1", "gate-router", nil},
		{"invalid_actor_key_empty", "", "", kkerrors.ErrActorInvalidActorKey},
		{"invalid_actor_key_slash", "game1", "game/player", kkerrors.ErrActorInvalidActorKey},
		{"invalid_node_slash", "game/1", "game_main", kkerrors.ErrActorInvalidNodeId},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := NewLucencyID(tt.nodeId, tt.actorKey)
			if tt.wantErr != nil {
				if err == nil || !errors.Is(err, tt.wantErr) {
					t.Errorf("NewLucencyID(%q, %q) = %v, want %v", tt.nodeId, tt.actorKey, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("NewLucencyID(%q, %q) err = %v", tt.nodeId, tt.actorKey, err)
				return
			}
			if id.NodeID() != tt.nodeId {
				t.Errorf("NodeID() = %q, want %q", id.NodeID(), tt.nodeId)
			}
			if id.ActorKey() != tt.actorKey {
				t.Errorf("ActorKey() = %q, want %q", id.ActorKey(), tt.actorKey)
			}
		})
	}
}

//------------------------------------------------------------------------------
// actor_locator_test
//------------------------------------------------------------------------------

func TestNewActorLocator(t *testing.T) {
	loc := NewLocalActorManager()
	if loc == nil {
		t.Fatal("NewActorLocator() returned nil")
	}
	loc2 := NewLocalActorManager("game1")
	if loc2 == nil {
		t.Fatal("NewActorLocator(game1) returned nil")
	}
}

func TestActorLocator_AddActor_AutoRegistersLocalNodeID(t *testing.T) {
	loc := NewLocalActorManager()
	actorSys := NewActorSystem()
	pid := actorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	defer actorSys.Root.Stop(pid)
	id, _ := NewLucencyID("solo", "a")
	if err := loc.AddActor(id, pid); err != nil {
		t.Fatalf("AddActor: %v", err)
	}
	local, err := loc.IsLocalActor(id)
	if err != nil || !local {
		t.Fatalf("IsLocalActor after AddActor-only = %v, %v", local, err)
	}
	var seen int
	loc.ForEachLocalNodeID(func(nodeID string) bool {
		if nodeID == "solo" {
			seen++
		}
		return true
	})
	if seen != 1 {
		t.Errorf("auto local node id count = %d, want 1", seen)
	}
}

func TestActorLocator_AddLocalNode_Idempotent(t *testing.T) {
	loc := NewLocalActorManager()
	actorSys := NewActorSystem()
	pid := actorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	defer actorSys.Root.Stop(pid)
	id, _ := NewLucencyID("solo", "a")
	_ = loc.AddActor(id, pid)
	if err := loc.AddLocalNode("solo"); err != nil {
		t.Fatalf("AddLocalNode: %v", err)
	}
	n := 0
	loc.ForEachLocalNodeID(func(nodeID string) bool {
		if nodeID == "solo" {
			n++
		}
		return true
	})
	if n != 1 {
		t.Errorf("local node id entries for solo = %d, want 1", n)
	}
}

func TestActorLocator_AddActor_GetActor_RemoveActor(t *testing.T) {
	actorSys := NewActorSystem()
	loc := NewLocalActorManager()

	id, err := NewLucencyID("game1", "test_actor")
	if err != nil {
		t.Fatalf("NewLucencyID: %v", err)
	}

	// GetActor before add returns nil
	pid, err := loc.GetActor(id)
	if err == nil || !errors.Is(err, kkerrors.ErrActorNotFound) {
		t.Fatalf("GetActor: %v", err)
	}
	if pid != nil {
		t.Errorf("GetActor before AddActor should return nil, got %v", pid)
	}

	// spawn echo actor
	echoProps := actor.PropsFromFunc(func(ctx actor.Context) {
		if ctx.Sender() != nil {
			ctx.Respond(ctx.Message())
		}
	})
	pid = actorSys.Root.Spawn(echoProps)
	defer actorSys.Root.Stop(pid)

	if err := loc.AddActor(id, pid); err != nil {
		t.Fatalf("AddActor: %v", err)
	}

	got, err := loc.GetActor(id)
	if err != nil {
		t.Fatalf("GetActor: %v", err)
	}
	if got == nil || got != pid {
		t.Errorf("GetActor after AddActor = %v, want %v", got, pid)
	}

	loc.RemoveActor(id)
	pid, err = loc.GetActor(id)
	if err == nil || !errors.Is(err, kkerrors.ErrActorNotFound) {
		t.Fatalf("GetActor: %v", err)
	}
	if pid != nil {
		t.Errorf("GetActor after RemoveActor should return nil, got %v", pid)
	}
}

func TestActorLocator_AddActor_Invalid(t *testing.T) {
	loc := NewLocalActorManager()
	actorSys := NewActorSystem()
	pid := actorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	defer actorSys.Root.Stop(pid)

	tests := []struct {
		name string
		id   LucencyID
		pid  *actor.PID
		want error
	}{
		{"invalid_key", LucencyID{nodeID: "", actorKey: "a/b"}, pid, kkerrors.ErrActorInvalidActorKey},
		{"invalid_empty_node", LucencyID{nodeID: "", actorKey: "ok"}, pid, kkerrors.ErrActorInvalidNodeId},
		{"invalid_node", LucencyID{nodeID: "a/b", actorKey: "ok"}, pid, kkerrors.ErrActorInvalidNodeId},
		{"valid_hyphenated_key", LucencyID{nodeID: "node-1", actorKey: "ok-key"}, pid, nil},
		{"nil_pid", LucencyID{nodeID: "game1", actorKey: "ok"}, nil, kkerrors.ErrActorAddInvalidPID},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := loc.AddActor(tt.id, tt.pid)
			if tt.want == nil {
				if err != nil {
					t.Errorf("AddActor = %v, want nil", err)
				}
				return
			}
			if err == nil || !errors.Is(err, tt.want) {
				t.Errorf("AddActor = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestActorLocator_IsLocalActor_IsRemoteActor(t *testing.T) {
	loc := NewLocalActorManager()
	if err := loc.AddLocalNode("game1"); err != nil {
		t.Fatalf("AddLocalNode: %v", err)
	}

	tests := []struct {
		name       string
		id         LucencyID
		wantLocal  bool
		wantRemote bool
	}{
		{"local_node_registered", mustLucencyID(t, "game1", "game_player"), true, false},
		{"remote_other_node", mustLucencyID(t, "game2", "game_player"), false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id := tt.id
			local, err := loc.IsLocalActor(id)
			if err != nil {
				t.Fatalf("IsLocalActor: %v", err)
			}
			if local != tt.wantLocal {
				t.Errorf("IsLocalActor() = %v, want %v", local, tt.wantLocal)
			}
			remote, err := loc.IsRemoteActor(id)
			if err != nil {
				t.Fatalf("IsRemoteActor: %v", err)
			}
			if remote != tt.wantRemote {
				t.Errorf("IsRemoteActor() = %v, want %v", remote, tt.wantRemote)
			}
		})
	}
}

func TestActorLocator_IsLocalActor_EmptyNodeID(t *testing.T) {
	loc := NewLocalActorManager("game1")
	id := LucencyID{nodeID: "", actorKey: "game_main"}
	_, err := loc.IsLocalActor(id)
	if err == nil || !errors.Is(err, kkerrors.ErrActorInvalidNodeId) {
		t.Fatalf("IsLocalActor(empty nodeID) err = %v, want %v", err, kkerrors.ErrActorInvalidNodeId)
	}
	_, err = loc.IsRemoteActor(id)
	if err == nil || !errors.Is(err, kkerrors.ErrActorInvalidNodeId) {
		t.Fatalf("IsRemoteActor(empty nodeID) err = %v, want %v", err, kkerrors.ErrActorInvalidNodeId)
	}
}

func TestActorLocator_AddLocalNode_RemoveLocalNode(t *testing.T) {
	loc := NewLocalActorManager()
	_ = loc.AddLocalNode("game1")

	idPre, _ := NewLucencyID("game1", "not_spawned_yet")
	local, _ := loc.IsLocalActor(idPre)
	if !local {
		t.Error("AddLocalNode alone should make any same-node LucencyID local for routing")
	}

	actorSys := NewActorSystem()
	id, _ := NewLucencyID("game1", "x")
	pid := actorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	defer actorSys.Root.Stop(pid)
	_ = loc.AddActor(id, pid)

	local, _ = loc.IsLocalActor(id)
	if !local {
		t.Error("registered actor should be local before RemoveLocalNode")
	}

	_ = loc.RemoveLocalNode("game1")
	local, _ = loc.IsLocalActor(id)
	if local {
		t.Error("actor of removed node should no longer be local")
	}
	if _, err := loc.GetActor(id); err == nil {
		t.Error("GetActor after RemoveLocalNode should fail")
	}
}

func TestActorLocator_ForEachLocalNodeID_ForEachActor(t *testing.T) {
	loc := NewLocalActorManager("game1")
	actorSys := NewActorSystem()

	id, _ := NewLucencyID("game1", "test")
	pid := actorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	_ = loc.AddActor(id, pid)
	defer actorSys.Root.Stop(pid)

	nodeCount := 0
	loc.ForEachLocalNodeID(func(nodeID string) bool {
		nodeCount++
		return nodeID == "game1"
	})
	if nodeCount != 1 {
		t.Errorf("ForEachLocalNodeID count = %d, want 1", nodeCount)
	}

	actorCount := 0
	loc.ForEachActor(func(id LucencyID, p *actor.PID) bool {
		actorCount++
		return true
	})
	if actorCount != 1 {
		t.Errorf("ForEachActor count = %d, want 1", actorCount)
	}
}
