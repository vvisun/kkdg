package testactor

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet/kkactor"
)

// 运行方式：
// go test ./tests/testactor -run=^$ -bench=. -benchmem

// BenchmarkActor_Spawn 测试创建 Actor 的性能
func BenchmarkActor_Spawn(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
			// 简单的空处理
		}))
		if pid == nil {
			b.Fatal("Spawn returned nil")
		}
	}
}

// BenchmarkActor_Tell 测试 Tell 消息发送性能
func BenchmarkActor_Tell(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		// 接收消息但不做处理，只测试发送性能
		_ = ctx.Message()
	}))
	if pid == nil {
		b.Fatal("Spawn returned nil")
	}

	// 等待 actor 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pid.Tell(i)
	}
}

// BenchmarkActor_Tell_Concurrent 测试并发 Tell 性能
func BenchmarkActor_Tell_Concurrent(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	var count int64
	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		atomic.AddInt64(&count, 1)
	}))
	if pid == nil {
		b.Fatal("Spawn returned nil")
	}

	// 等待 actor 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pid.Tell(1)
		}
	})

	// 等待所有消息处理完成
	time.Sleep(100 * time.Millisecond)
}

// BenchmarkActor_Ask 测试 Ask/Reply 性能
func BenchmarkActor_Ask(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		// 回复发送者
		if ctx.Sender() != nil {
			ctx.Send(ctx.Sender(), ctx.Message())
		}
	}))
	if pid == nil {
		b.Fatal("Spawn returned nil")
	}

	root := kkactor.NewRootContext(sys)

	// 等待 actor 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, ok := root.Ask(pid, i, time.Second)
		if !ok {
			b.Fatalf("Ask timeout at iteration %d", i)
		}
	}
}

// BenchmarkActor_Ask_Concurrent 测试并发 Ask 性能
func BenchmarkActor_Ask_Concurrent(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		// 回复发送者
		if ctx.Sender() != nil {
			ctx.Send(ctx.Sender(), ctx.Message())
		}
	}))
	if pid == nil {
		b.Fatal("Spawn returned nil")
	}

	root := kkactor.NewRootContext(sys)

	// 等待 actor 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, ok := root.Ask(pid, i, time.Second)
			if !ok {
				b.Errorf("Ask timeout at iteration %d", i)
			}
			i++
		}
	})
}

// BenchmarkActor_MultipleActors 测试多个 Actor 的消息处理性能
func BenchmarkActor_MultipleActors(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	const numActors = 100
	pids := make([]*kkactor.PID, numActors)
	for i := 0; i < numActors; i++ {
		pids[i] = sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
			_ = ctx.Message()
		}))
		if pids[i] == nil {
			b.Fatalf("Spawn returned nil at index %d", i)
		}
	}

	// 等待所有 actors 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pids[i%numActors].Tell(i)
	}
}

// BenchmarkActor_MessageThroughput 测试消息吞吐量
func BenchmarkActor_MessageThroughput(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	var received int64
	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		atomic.AddInt64(&received, 1)
	}))
	if pid == nil {
		b.Fatal("Spawn returned nil")
	}

	// 等待 actor 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pid.Tell(i)
	}

	// 等待所有消息处理完成
	time.Sleep(200 * time.Millisecond)
	if atomic.LoadInt64(&received) != int64(b.N) {
		b.Logf("Expected %d messages, received %d", b.N, atomic.LoadInt64(&received))
	}
}

// BenchmarkActor_MessageThroughput_Concurrent 测试并发消息吞吐量
func BenchmarkActor_MessageThroughput_Concurrent(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	var received int64
	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		atomic.AddInt64(&received, 1)
	}))
	if pid == nil {
		b.Fatal("Spawn returned nil")
	}

	// 等待 actor 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pid.Tell(1)
		}
	})

	// 等待所有消息处理完成
	time.Sleep(500 * time.Millisecond)
}

// BenchmarkActor_Chain 测试 Actor 链式消息传递性能
func BenchmarkActor_Chain(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	var finalCount int64
	// 创建链式 actors
	var prevPID *kkactor.PID
	for i := 0; i < 10; i++ {
		prev := prevPID
		pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
			if prev != nil {
				prev.Tell(ctx.Message())
			} else {
				atomic.AddInt64(&finalCount, 1)
			}
		}))
		if pid == nil {
			b.Fatalf("Spawn returned nil at chain level %d", i)
		}
		prevPID = pid
	}

	// 等待所有 actors 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		prevPID.Tell(i)
	}

	// 等待所有消息处理完成
	time.Sleep(200 * time.Millisecond)
}

// BenchmarkActor_Timer 测试 Timer 创建和取消性能
func BenchmarkActor_Timer(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	var timerIDs []kkactor.TimerID
	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		switch m := ctx.Message().(type) {
		case string:
			if m == "schedule" {
				id := ctx.After(1*time.Hour, "timeout")
				timerIDs = append(timerIDs, id)
			} else if m == "cancel" {
				if len(timerIDs) > 0 {
					ctx.CancelTimer(timerIDs[0])
					timerIDs = timerIDs[1:]
				}
			}
		}
	}))
	if pid == nil {
		b.Fatal("Spawn returned nil")
	}

	// 等待 actor 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pid.Tell("schedule")
		pid.Tell("cancel")
	}
}

// BenchmarkActor_ManyActors_Tell 测试大量 Actor 下的 Tell 性能
func BenchmarkActor_ManyActors_Tell(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	const numActors = 1000
	pids := make([]*kkactor.PID, numActors)
	for i := 0; i < numActors; i++ {
		pids[i] = sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
			_ = ctx.Message()
		}))
		if pids[i] == nil {
			b.Fatalf("Spawn returned nil at index %d", i)
		}
	}

	// 等待所有 actors 启动
	time.Sleep(50 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pids[i%numActors].Tell(i)
	}
}

// BenchmarkActor_ConcurrentSpawn 测试并发创建 Actor 的性能
func BenchmarkActor_ConcurrentSpawn(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
				_ = ctx.Message()
			}))
			if pid == nil {
				b.Error("Spawn returned nil")
			}
		}
	})
}

// BenchmarkActor_MessageSize 测试不同消息大小的性能
func BenchmarkActor_MessageSize_Small(b *testing.B) {
	benchmarkMessageSize(b, 10)
}

func BenchmarkActor_MessageSize_Medium(b *testing.B) {
	benchmarkMessageSize(b, 1024)
}

func BenchmarkActor_MessageSize_Large(b *testing.B) {
	benchmarkMessageSize(b, 1024*1024)
}

func benchmarkMessageSize(b *testing.B, size int) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	msg := make([]byte, size)
	pid := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		_ = ctx.Message()
	}))
	if pid == nil {
		b.Fatal("Spawn returned nil")
	}

	// 等待 actor 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		pid.Tell(msg)
	}
}

// BenchmarkActor_FanOut 测试一个 Actor 向多个 Actor 发送消息的性能
func BenchmarkActor_FanOut(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	const numTargets = 100
	targets := make([]*kkactor.PID, numTargets)
	for i := 0; i < numTargets; i++ {
		targets[i] = sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
			_ = ctx.Message()
		}))
		if targets[i] == nil {
			b.Fatalf("Spawn returned nil at index %d", i)
		}
	}

	// 创建发送者 actor
	sender := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		msg := ctx.Message()
		for _, target := range targets {
			target.Tell(msg)
		}
	}))
	if sender == nil {
		b.Fatal("Spawn sender returned nil")
	}

	// 等待所有 actors 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sender.Tell(i)
	}
}

// BenchmarkActor_FanIn 测试多个 Actor 向一个 Actor 发送消息的性能
func BenchmarkActor_FanIn(b *testing.B) {
	sys := kkactor.NewActorSystem()
	defer sys.Stop()

	var received int64
	receiver := sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		atomic.AddInt64(&received, 1)
	}))
	if receiver == nil {
		b.Fatal("Spawn receiver returned nil")
	}

	const numSenders = 100
	senders := make([]*kkactor.PID, numSenders)
	for i := 0; i < numSenders; i++ {
		senders[i] = sys.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
			receiver.Tell(ctx.Message())
		}))
		if senders[i] == nil {
			b.Fatalf("Spawn sender returned nil at index %d", i)
		}
	}

	// 等待所有 actors 启动
	time.Sleep(10 * time.Millisecond)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			senders[i%numSenders].Tell(i)
			i++
		}
	})

	// 等待所有消息处理完成
	time.Sleep(500 * time.Millisecond)
}
