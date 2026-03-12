package kkactor

import (
	"sync"
	"testing"

	"github.com/asynkron/protoactor-go/actor"
)

// noopActor is a lightweight actor used for local throughput benchmarks.
type noopActor struct{}

func (n *noopActor) Receive(ctx actor.Context) {
	// intentionally empty: we only measure protoactor's messaging overhead
}

// BenchmarkProtoactorLocalSend measures the throughput of sending messages
// between the root context and a single local actor (one producer).
func BenchmarkProtoactorLocalSend(b *testing.B) {
	sys := NewSilentActorSystem()
	props := actor.PropsFromProducer(func() actor.Actor { return &noopActor{} })
	pid := sys.Root.Spawn(props)
	defer sys.Root.Stop(pid)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		sys.Root.Send(pid, i)
	}
}

// benchmarkProtoactorFanIn measures fan-in performance where multiple goroutines
// concurrently send messages to a single local actor.
func benchmarkProtoactorFanIn(b *testing.B, workers int) {
	sys := NewSilentActorSystem()
	props := actor.PropsFromProducer(func() actor.Actor { return &noopActor{} })
	pid := sys.Root.Spawn(props)
	defer sys.Root.Stop(pid)

	if workers <= 0 {
		workers = 1
	}

	total := b.N
	base := total / workers
	rem := total % workers

	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		// distribute remainder across first rem workers
		count := base
		if w < rem {
			count++
		}
		go func(n int) {
			defer wg.Done()
			for i := 0; i < n; i++ {
				sys.Root.Send(pid, i)
			}
		}(count)
	}
	wg.Wait()
}

func BenchmarkProtoactorFanIn_1(b *testing.B)  { benchmarkProtoactorFanIn(b, 1) }
func BenchmarkProtoactorFanIn_4(b *testing.B)  { benchmarkProtoactorFanIn(b, 4) }
func BenchmarkProtoactorFanIn_16(b *testing.B) { benchmarkProtoactorFanIn(b, 16) }
func BenchmarkProtoactorFanIn_64(b *testing.B) { benchmarkProtoactorFanIn(b, 64) }

