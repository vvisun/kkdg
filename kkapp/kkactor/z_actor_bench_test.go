package kkactor

import (
	"testing"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
)

//------------------------------------------------------------------------------
// actor_id benchmarks
//------------------------------------------------------------------------------

var mapKKK = make(map[LucencyActorID]int)

// 测试以LucencyActorID为key的map性能
func BenchmarkMapLucencyActorID(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mapKKK[LucencyActorID{nodeID: "game1", actorKey: "game_player"}] = i
	}
}

// 测试NewLucencyActorID为key的map 增 删 查 性能
func BenchmarkMapNewLucencyActorID(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id, _ := NewLucencyActorID("game1", "game_player")
		mapKKK[id] = i
		delete(mapKKK, id)
		_, _ = mapKKK[id]
	}
}

func BenchmarkNewLucencyActorID(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = NewLucencyActorID("game1", "game_player")
	}
}

//------------------------------------------------------------------------------
// actor_locator benchmarks
//------------------------------------------------------------------------------

func BenchmarkActorLocator_AddActor(b *testing.B) {
	actorSys := NewSilentActorSystem()
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
		// _ = loc.AddActorEx("", "bench_actor", pids[i%poolSize])
	}
}

func BenchmarkActorLocator_GetActor(b *testing.B) {
	actorSys := NewSilentActorSystem()
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
	actorSys := NewSilentActorSystem()
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
