package kkactor

import (
	"errors"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kkerrors"
)

type stubRemoteTransport struct {
	startErr    error
	startCalls  int
	closeCalls  int
	receiverSet actorremotes.IRemoteActorReceiver
	registrySet *actorremotes.MessageRegistry
}

func (t *stubRemoteTransport) Start() error {
	t.startCalls++
	return t.startErr
}

func (t *stubRemoteTransport) Close() error {
	t.closeCalls++
	return nil
}

func (t *stubRemoteTransport) SetReceiver(receiver actorremotes.IRemoteActorReceiver) {
	t.receiverSet = receiver
}

func (t *stubRemoteTransport) Send(target actorremotes.ActorRef, msg any) error {
	return nil
}

func (t *stubRemoteTransport) Request(target actorremotes.ActorRef, msg any, timeout time.Duration) (any, error) {
	return nil, nil
}

func (t *stubRemoteTransport) RequestAsync(target actorremotes.ActorRef, msg any, timeout time.Duration, callback func(result any, err error)) error {
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
		{"valid_empty_node", "", "game_main", nil},
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
			id, err := NewLucencyActorID(tt.nodeId, tt.actorKey)
			if tt.wantErr != nil {
				if err == nil || !errors.Is(err, tt.wantErr) {
					t.Errorf("NewLucencyActorID(%q, %q) = %v, want %v", tt.nodeId, tt.actorKey, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Errorf("NewLucencyActorID(%q, %q) err = %v", tt.nodeId, tt.actorKey, err)
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
	loc := NewActorLocator()
	if loc == nil {
		t.Fatal("NewActorLocator() returned nil")
	}
	// with local nodes
	node1 := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "")
	loc2 := NewActorLocator(node1)
	if loc2 == nil {
		t.Fatal("NewActorLocator(node1) returned nil")
	}
}

func TestActorLocator_AddActor_GetActor_RemoveActor(t *testing.T) {
	actorSys := NewActorSystem()
	loc := NewActorLocator()

	id, err := NewLucencyActorID("", "test_actor")
	if err != nil {
		t.Fatalf("NewLucencyActorID: %v", err)
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
	loc := NewActorLocator()
	actorSys := NewActorSystem()
	pid := actorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	defer actorSys.Root.Stop(pid)

	tests := []struct {
		name string
		id   LucencyActorID
		pid  *actor.PID
		want error
	}{
		{"invalid_key", LucencyActorID{nodeID: "", actorKey: "a/b"}, pid, kkerrors.ErrActorInvalidActorKey},
		{"invalid_node", LucencyActorID{nodeID: "a/b", actorKey: "ok"}, pid, kkerrors.ErrActorInvalidNodeId},
		{"valid_hyphenated_key", LucencyActorID{nodeID: "node-1", actorKey: "ok-key"}, pid, nil},
		{"nil_pid", LucencyActorID{nodeID: "", actorKey: "ok"}, nil, kkerrors.ErrActorAddInvalidPID},
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
	node1 := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "")
	loc := NewActorLocator(node1)

	tests := []struct {
		name       string
		nodeId     string
		actorKey   string
		wantLocal  bool
		wantRemote bool
	}{
		{"local_empty_node", "", "game_main", true, false},
		{"local_same_node", "game1", "game_player", true, false},
		{"remote_other_node", "game2", "game_player", false, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, err := NewLucencyActorID(tt.nodeId, tt.actorKey)
			if err != nil {
				t.Fatalf("NewLucencyActorID: %v", err)
			}
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

func TestActorLocator_AddNode_RemoveNode(t *testing.T) {
	loc := NewActorLocator()
	node1 := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "")
	loc.AddNode(node1)

	local, _ := loc.IsLocalActor(LucencyActorID{nodeID: "game1", actorKey: "x"})
	if !local {
		t.Error("game1 should be local after AddNode")
	}

	loc.RemoveNode(node1)
	local, _ = loc.IsLocalActor(LucencyActorID{nodeID: "game1", actorKey: "x"})
	if local {
		t.Error("game1 should be remote after RemoveNode")
	}
}

func TestActorLocator_ForEachNode_ForEachActor(t *testing.T) {
	node1 := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "")
	loc := NewActorLocator(node1)
	actorSys := NewActorSystem()

	id, _ := NewLucencyActorID("", "test")
	pid := actorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	_ = loc.AddActor(id, pid)
	defer actorSys.Root.Stop(pid)

	nodeCount := 0
	loc.ForEachNode(func(node *kkapp.NodeInfo) bool {
		nodeCount++
		return node.GetNodeId() == "game1"
	})
	if nodeCount != 1 {
		t.Errorf("ForEachNode count = %d, want 1", nodeCount)
	}

	actorCount := 0
	loc.ForEachActor(func(id LucencyActorID, p *actor.PID) bool {
		actorCount++
		return true
	})
	if actorCount != 1 {
		t.Errorf("ForEachActor count = %d, want 1", actorCount)
	}
}
