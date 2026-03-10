package kkactor

import (
	"fmt"
	"testing"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
)

func TestGetActorName(t *testing.T) {
	tests := []struct {
		id   LucencyActorID
		want string
	}{
		{LucencyActorID{nodeID: "game1", actorKey: "ccgame/main"}, "game1/ccgame/main"},
		{LucencyActorID{nodeID: "", actorKey: "gate/router"}, "/gate/router"},
		{LucencyActorID{nodeID: "node1", actorKey: "a"}, "node1/a"},
	}
	for _, tt := range tests {
		got := GetActorName(tt.id)
		if got != tt.want {
			t.Errorf("GetActorName(%+v) = %q, want %q", tt.id, got, tt.want)
		}
	}
}

func TestGetActorId(t *testing.T) {
	tests := []struct {
		actorName string
		want      LucencyActorID
	}{
		{"game1/ccgame/main", LucencyActorID{nodeID: "game1", actorKey: "ccgame/main"}},
		{"gate/router", LucencyActorID{nodeID: "gate", actorKey: "router"}}, // 第1个/前为 NodeID，后为 ActorKey
		{"n1/a/b", LucencyActorID{nodeID: "n1", actorKey: "a/b"}},
		{"", LucencyActorID{nodeID: "", actorKey: ""}},
		{"localOnly", LucencyActorID{nodeID: "", actorKey: "localOnly"}}, // 无分隔符时整串为 ActorKey
	}
	for _, tt := range tests {
		got := GetActorId(tt.actorName)
		if got != tt.want {
			t.Errorf("GetActorId(%q) = %+v, want %+v", tt.actorName, got, tt.want)
		}
	}
}

func TestGetActorId_GetActorName_RoundTrip(t *testing.T) {
	ids := []LucencyActorID{
		{nodeID: "game1", actorKey: "ccgame/main"},
		{nodeID: "", actorKey: "local/only"},
		{nodeID: "gate1", actorKey: "gate/router"},
	}
	for _, id := range ids {
		name := GetActorName(id)
		back := GetActorId(name)
		if back != id {
			t.Errorf("GetActorId(GetActorName(%+v)) = %+v", id, back)
		}
	}
}

func TestNewActorLocator_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewActorLocator(nil) should panic")
		}
	}()
	NewActorLocator(nil)
}

func TestActorLocator_Add_Locate_Remove(t *testing.T) {
	sys := actor.NewActorSystem()
	pid := sys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	node := kkapp.NewNodeInfo("node1", "game", "127.0.0.1:8080", "", nil)
	loc := NewActorLocator(node)

	id := LucencyActorID{nodeID: "node1", actorKey: "ccgame/main"}
	if got := loc.GetActor(id); got != nil {
		t.Errorf("LocateActor before Add = %v, want nil", got)
	}

	loc.AddActor(id, pid)
	got := loc.GetActor(id)
	if got == nil || got.String() != pid.String() {
		t.Errorf("LocateActor after Add = %v, want %v", got, pid)
	}

	loc.RemoveActor(id)
	if got := loc.GetActor(id); got != nil {
		t.Errorf("LocateActor after Remove = %v, want nil", got)
	}
}

func TestActorLocator_IsLocalActor_IsRemoteActor(t *testing.T) {
	node := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "", nil)
	loc := NewActorLocator(node)

	tests := []struct {
		id       LucencyActorID
		isLocal  bool
		isRemote bool
	}{
		{LucencyActorID{nodeID: "game1", actorKey: "x"}, true, false},
		{LucencyActorID{nodeID: "", actorKey: "x"}, true, false},
		{LucencyActorID{nodeID: "game2", actorKey: "x"}, false, true},
		{LucencyActorID{nodeID: "gate1", actorKey: "y"}, false, true},
	}
	for _, tt := range tests {
		if got := loc.IsLocalActor(tt.id); got != tt.isLocal {
			t.Errorf("IsLocalActor(%+v) = %v, want %v", tt.id, got, tt.isLocal)
		}
		if got := loc.IsRemoteActor(tt.id); got != tt.isRemote {
			t.Errorf("IsRemoteActor(%+v) = %v, want %v", tt.id, got, tt.isRemote)
		}
	}
}

func TestActorLocator_IsLocalActorName_IsRemoteActorName(t *testing.T) {
	node := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "", nil)
	loc := NewActorLocator(node)

	tests := []struct {
		actorName string
		isLocal   bool
		isRemote  bool
	}{
		{"game1/ccgame/main", true, false},
		{"/local/only", true, false},
		{"game2/ccgame/main", false, true},
		{"gate1/router", false, true},
	}
	for _, tt := range tests {
		if got := loc.IsLocalActorName(tt.actorName); got != tt.isLocal {
			t.Errorf("IsLocalActorName(%q) = %v, want %v", tt.actorName, got, tt.isLocal)
		}
		if got := loc.IsRemoteActorName(tt.actorName); got != tt.isRemote {
			t.Errorf("IsRemoteActorName(%q) = %v, want %v", tt.actorName, got, tt.isRemote)
		}
	}
}

func TestActorLocator_ConcurrentAddRemoveLocate(t *testing.T) {
	sys := actor.NewActorSystem()
	pid := sys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	node := kkapp.NewNodeInfo("n1", "t", "127.0.0.1:8080", "", nil)
	loc := NewActorLocator(node)

	// 并发 Add/Locate/Remove 不同 key，不应 race
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			id := LucencyActorID{nodeID: "n1", actorKey: "a/1"}
			loc.AddActor(id, pid)
			loc.GetActor(id)
		}
		done <- struct{}{}
	}()
	go func() {
		for i := 0; i < 100; i++ {
			id := LucencyActorID{nodeID: "n1", actorKey: "a/2"}
			loc.AddActor(id, pid)
			loc.RemoveActor(id)
		}
		done <- struct{}{}
	}()
	<-done
	<-done
}

//------------------------------ benchmark --------------------------------

var (
	benchID       = LucencyActorID{nodeID: "game1", actorKey: "ccgame/main"}
	benchName     = "game1/ccgame/main"
	benchLocator  *ActorLocator
	benchPID      *actor.PID
	benchActorSys *actor.ActorSystem
)

func init() {
	benchActorSys = actor.NewActorSystem()
	benchPID = benchActorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	node := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "", nil)
	benchLocator = NewActorLocator(node)
	for i := 0; i < 1000; i++ {
		id := LucencyActorID{nodeID: "game1", actorKey: fmt.Sprintf("comp/%d", i)}
		benchLocator.AddActor(id, benchPID)
	}
}

func BenchmarkGetActorName(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = GetActorName(benchID)
	}
}

func BenchmarkGetActorId(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = GetActorId(benchName)
	}
}

func BenchmarkActorLocator_LocateActor(b *testing.B) {
	id := LucencyActorID{nodeID: "game1", actorKey: "comp/0"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = benchLocator.GetActor(id)
	}
}

func BenchmarkActorLocator_LocateActor_Miss(b *testing.B) {
	id := LucencyActorID{nodeID: "game1", actorKey: "nonexistent"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = benchLocator.GetActor(id)
	}
}

func BenchmarkActorLocator_AddActor(b *testing.B) {
	node := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "", nil)
	loc := NewActorLocator(node)
	id := LucencyActorID{nodeID: "game1", actorKey: "ccgame/main"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loc.AddActor(id, benchPID)
	}
}

func BenchmarkActorLocator_RemoveActor(b *testing.B) {
	node := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "", nil)
	loc := NewActorLocator(node)
	for i := 0; i < 1000; i++ {
		loc.AddActor(LucencyActorID{nodeID: "game1", actorKey: fmt.Sprintf("x/%d", i)}, benchPID)
	}
	id := LucencyActorID{nodeID: "game1", actorKey: "x/0"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		loc.RemoveActor(id)
		loc.AddActor(id, benchPID) // 补回以便下次 Remove
	}
}

func BenchmarkActorLocator_IsLocalActor(b *testing.B) {
	id := LucencyActorID{nodeID: "game1", actorKey: "ccgame/main"}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = benchLocator.IsLocalActor(id)
	}
}

func BenchmarkActorLocator_IsLocalActorName(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = benchLocator.IsLocalActorName(benchName)
	}
}

func BenchmarkActorLocator_IsRemoteActorName(b *testing.B) {
	name := "game2/ccgame/main"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = benchLocator.IsRemoteActorName(name)
	}
}
