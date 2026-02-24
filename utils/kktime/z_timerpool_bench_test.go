package kktime

import (
	"sync"
	"testing"
	"time"
)

const benchDuration = 1 * time.Hour // 仅测 Get/Put 开销，不触发 timer

// BenchmarkTimerPool_GetPut Get+Put 性能（取 timer 后立即 Stop 再 Put，不等待触发）
func BenchmarkTimerPool_GetPut(b *testing.B) {
	pool := GetGlobalTimerPool()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		t := pool.Get(benchDuration)
		t.Stop()
		pool.Put(t)
	}
}

// BenchmarkTimerPool_GetPut_Reuse 池复用场景：预填充后反复 Get/Put
func BenchmarkTimerPool_GetPut_Reuse(b *testing.B) {
	pool := GetGlobalTimerPool()
	for i := 0; i < 128; i++ {
		t := pool.Get(benchDuration)
		t.Stop()
		pool.Put(t)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		t := pool.Get(benchDuration)
		t.Stop()
		pool.Put(t)
	}
}

// BenchmarkTimerPool_GetOnly 仅 Get（不归还），衡量从池取与新建的开销
func BenchmarkTimerPool_GetOnly(b *testing.B) {
	pool := GetGlobalTimerPool()
	for i := 0; i < 256; i++ {
		t := pool.Get(benchDuration)
		t.Stop()
		pool.Put(t)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		t := pool.Get(benchDuration)
		t.Stop()
		_ = t
	}
}

// BenchmarkTimerPool_Concurrent_GetPut 并发 Get+Put
func BenchmarkTimerPool_Concurrent_GetPut(b *testing.B) {
	pool := GetGlobalTimerPool()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			t := pool.Get(benchDuration)
			t.Stop()
			pool.Put(t)
		}
	})
}

// BenchmarkTimerPool_NewTimerVsPool time.NewTimer+Stop 与 Pool Get+Stop+Put 对比
func BenchmarkTimerPool_NewTimerVsPool(b *testing.B) {
	pool := GetGlobalTimerPool()
	d := benchDuration

	b.Run("NewTimer", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			t := time.NewTimer(d)
			t.Stop()
		}
	})

	b.Run("Pool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			t := pool.Get(d)
			t.Stop()
			pool.Put(t)
		}
	})
}

// BenchmarkTimerPool_Put_AfterFire Get(0)+<-C+Put 全流程（已触发 timer 的 Put 路径）
func BenchmarkTimerPool_Put_AfterFire(b *testing.B) {
	pool := GetGlobalTimerPool()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		t := pool.Get(0)
		<-t.C
		pool.Put(t)
	}
}

// BenchmarkTimerPool_HighContention 高争用：多 goroutine 同时 Get/Put
func BenchmarkTimerPool_HighContention(b *testing.B) {
	pool := GetGlobalTimerPool()
	const numGoroutines = 100

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	n := b.N / numGoroutines
	if n < 1 {
		n = 1
	}
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				t := pool.Get(benchDuration)
				t.Stop()
				pool.Put(t)
			}
		}()
	}
	wg.Wait()
}
