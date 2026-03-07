package kkprocessor

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// Benchmark workerQueue Push 吞吐：仅入队 + 启动 worker，任务为空操作。
func BenchmarkWorkerQueue_PushNoOp(b *testing.B) {
	var chk int64 = 0
	var nnn int64 = 0
	wq := NewWorkerQueue(1)
	noOp := func() {
		preNNN := nnn
		nnn++
		if nnn != preNNN+1 {
			fmt.Println("[error] nnn", nnn, preNNN)
		}
		pre := atomic.LoadInt64(&chk)
		n := atomic.AddInt64(&chk, 1)
		if n != pre+1 {
			fmt.Println("[error] chk", n, pre)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wq.Push(noOp)
	}
}

// Benchmark workerQueue 端到端：每轮 push 一批并等待完成，maxConcurrency=1。
func BenchmarkWorkerQueue_ThroughputConcurrency1(b *testing.B) {
	const batch = 64
	wq := NewWorkerQueue(1)
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		wg.Add(batch)
		for i := 0; i < batch; i++ {
			wq.Push(func() { wg.Done() })
		}
		wg.Wait()
	}
}

// Benchmark workerQueue 端到端：每轮 push 一批并等待完成，maxConcurrency=4。
func BenchmarkWorkerQueue_ThroughputConcurrency4(b *testing.B) {
	const batch = 64
	wq := NewWorkerQueue(4)
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		wg.Add(batch)
		for i := 0; i < batch; i++ {
			wq.Push(func() { wg.Done() })
		}
		wg.Wait()
	}
}

// Benchmark workerQueue 端到端：每轮 push 一批并等待完成，maxConcurrency=16。
func BenchmarkWorkerQueue_ThroughputConcurrency16(b *testing.B) {
	const batch = 64
	wq := NewWorkerQueue(16)
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		wg.Add(batch)
		for i := 0; i < batch; i++ {
			wq.Push(func() { wg.Done() })
		}
		wg.Wait()
	}
}

// Benchmark workerQueue 批量 Push 后等待全部执行完（模拟 TaskReadProcessor 场景）。
func BenchmarkWorkerQueue_BatchPushThenWait(b *testing.B) {
	const batch = 64
	wq := NewWorkerQueue(4)
	var wg sync.WaitGroup
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		wg.Add(batch)
		for i := 0; i < batch; i++ {
			wq.Push(func() { wg.Done() })
		}
		wg.Wait()
	}
}
