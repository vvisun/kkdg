package kkactor

import (
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
)

//------------------------------------------------------------------------------
// framework benchmarks
//------------------------------------------------------------------------------

func setupBenchFramework(b *testing.B) (*ActorFramework, LucencyActorID, func()) {
	af := NewActorFramework()
	actorSys := af.GetActorSystem()
	loc := af.GetLocator()
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
		_, _ = af.Request(id, req, 5*time.Second)
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
