package kkactor

import (
	"testing"
	"time"
)

type benchActor struct{}

func (benchActor) Receive(ctx Context) {
	switch msg := ctx.Message().(type) {
	case string:
		if msg == "ping" {
			ctx.Respond("pong")
		}
	default:
		_ = msg
	}
}

func BenchmarkActorSendLocal(b *testing.B) {
	sys := NewActorSystem()
	root := NewRootContext(sys)
	pid := sys.Spawn(FromProducer(func() Actor { return benchActor{} }))
	if pid == nil {
		b.Fatalf("spawn returned nil pid")
	}
	b.Cleanup(func() {
		sys.Stop()
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		root.Send(pid, i)
	}
}

func BenchmarkActorRequestLocal(b *testing.B) {
	sys := NewActorSystem()
	root := NewRootContext(sys)
	pid := sys.Spawn(FromProducer(func() Actor { return benchActor{} }))
	if pid == nil {
		b.Fatalf("spawn returned nil pid")
	}
	b.Cleanup(func() {
		sys.Stop()
	})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		fut := root.RequestFuture(pid, "ping", 5*time.Second)
		if _, err := fut.Result(); err != nil {
			b.Fatalf("request failed: %v", err)
		}
	}
}
