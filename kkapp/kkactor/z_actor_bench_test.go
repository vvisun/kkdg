package kkactor

import (
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
)

//------------------------------------------------------------------------------
// actor_id benchmarks
//------------------------------------------------------------------------------

func BenchmarkNewLucencyActorID(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewLucencyActorID("game1", "game_player")
	}
}

func BenchmarkCombineActorKeys(b *testing.B) {
	keys := []string{"gate", "router", "session"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CombineActorKeys(keys...)
	}
}

func BenchmarkCombineNodeAndActorKey(b *testing.B) {
	keys := []string{"game", "player"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = CombineNodeAndActorKey("game1", keys...)
	}
}

func BenchmarkGetActorName(b *testing.B) {
	id, _ := NewLucencyActorID("game1", "game_player")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetActorName(id)
	}
}

func BenchmarkGetActorId(b *testing.B) {
	actorName := "game1/game_player"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = GetActorId(actorName)
	}
}

//------------------------------------------------------------------------------
// actor_locator benchmarks
//------------------------------------------------------------------------------

func BenchmarkActorLocator_AddActor(b *testing.B) {
	actorSys := NewActorSystem()
	loc := NewActorLocator()
	echoProps := actor.PropsFromFunc(func(ctx actor.Context) {
		if ctx.Sender() != nil {
			ctx.Respond(ctx.Message())
		}
	})

	// Pre-spawn a pool of actors to avoid spawn overhead in loop
	const poolSize = 1000
	pids := make([]*actor.PID, poolSize)
	for i := 0; i < poolSize; i++ {
		pids[i] = actorSys.Root.Spawn(echoProps)
	}
	defer func() {
		for _, p := range pids {
			actorSys.Root.Stop(p)
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, _ := NewLucencyActorID("", "bench_actor")
		_ = loc.AddActor(id, pids[i%poolSize])
	}
}

func BenchmarkActorLocator_GetActor(b *testing.B) {
	actorSys := NewActorSystem()
	loc := NewActorLocator()
	id, _ := NewLucencyActorID("", "bench_actor")
	pid := actorSys.Root.Spawn(actor.PropsFromFunc(func(ctx actor.Context) {}))
	defer actorSys.Root.Stop(pid)
	_ = loc.AddActor(id, pid)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = loc.GetActor(id)
	}
}

func BenchmarkActorLocator_IsLocalActor(b *testing.B) {
	node1 := kkapp.NewNodeInfo("game1", "game", "127.0.0.1:8080", "", nil)
	loc := NewActorLocator(node1)
	id, _ := NewLucencyActorID("game1", "game_player")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = loc.IsLocalActor(id)
	}
}

func BenchmarkActorLocator_AddGetRemove(b *testing.B) {
	actorSys := NewActorSystem()
	loc := NewActorLocator()
	echoProps := actor.PropsFromFunc(func(ctx actor.Context) {
		if ctx.Sender() != nil {
			ctx.Respond(ctx.Message())
		}
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, _ := NewLucencyActorID("", "bench_actor")
		pid := actorSys.Root.Spawn(echoProps)
		_ = loc.AddActor(id, pid)
		_, _ = loc.GetActor(id)
		loc.RemoveActor(id)
		actorSys.Root.Stop(pid)
	}
}

//------------------------------------------------------------------------------
// framework benchmarks
//------------------------------------------------------------------------------

func setupBenchFramework(b *testing.B) (*ActorFramework, LucencyActorID, func()) {
	actorSys := NewActorSystem()
	loc := NewActorLocator()
	af := NewActorFramework(loc, actorSys)
	id, _ := NewLucencyActorID("", "echo")
	echoProps := actor.PropsFromFunc(func(ctx actor.Context) {
		if ctx.Sender() != nil {
			ctx.Respond(ctx.Message())
		}
	})
	pid := actorSys.Root.Spawn(echoProps)
	_ = loc.AddActor(id, pid)
	cleanup := func() {
		actorSys.Root.Stop(pid)
	}
	return af, id, cleanup
}

func BenchmarkActorFramework_Send(b *testing.B) {
	af, id, cleanup := setupBenchFramework(b)
	defer cleanup()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = af.Send(id, "hello")
	}
}

func BenchmarkActorFramework_Request(b *testing.B) {
	af, id, cleanup := setupBenchFramework(b)
	defer cleanup()

	type msg struct{ V int }
	req := &msg{V: 42}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = af.Request(id, req, 5*time.Second)
	}
}

func BenchmarkRequest_Generic(b *testing.B) {
	af, id, cleanup := setupBenchFramework(b)
	defer cleanup()

	type Msg struct{ V int }
	req := &Msg{V: 42}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = Request[Msg, Msg](af, id, req, 5*time.Second)
	}
}

func BenchmarkActorFramework_Request_Parallel(b *testing.B) {
	af, id, cleanup := setupBenchFramework(b)
	defer cleanup()

	type msg struct{ V int }

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		req := &msg{V: 42}
		for pb.Next() {
			_, _ = af.Request(id, req, 5*time.Second)
		}
	})
}
