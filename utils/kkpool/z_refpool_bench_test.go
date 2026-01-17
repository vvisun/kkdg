package kkpool

import (
	"sync"
	"testing"
)

// BenchmarkRefPool_Get_Put Get/Put操作性能测试
func BenchmarkRefPool_Get_Put(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		obj := pool.Get()
		pool.Put(obj)
	}
}

// BenchmarkRefPool_GetOnly 只测试Get操作
func BenchmarkRefPool_GetOnly(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	// 预先放入一些对象
	for i := 0; i < 1000; i++ {
		obj := pool.Get()
		pool.Put(obj)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		obj := pool.Get()
		pool.Put(obj)
	}
}

// BenchmarkRefPool_Size Size操作性能测试
func BenchmarkRefPool_Size(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = pool.Size()
	}
}

// BenchmarkRefPool_GetStats GetStats操作性能测试
func BenchmarkRefPool_GetStats(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = pool.GetStats()
	}
}

// BenchmarkRefPool_Concurrent_GetPut 并发Get/Put测试
func BenchmarkRefPool_Concurrent_GetPut(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			obj := pool.Get()
			pool.Put(obj)
		}
	})
}

// BenchmarkRefPool_Concurrent_ReadSize 并发读取Size
func BenchmarkRefPool_Concurrent_ReadSize(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = pool.Size()
		}
	})
}

// BenchmarkRefPool_HighContention 高争用场景
func BenchmarkRefPool_HighContention(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	const numGoroutines = 100
	const operationsPerGoroutine = 1000

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < operationsPerGoroutine; i++ {
				obj := pool.Get()
				// 模拟一些处理时间
				_ = obj.ID
				pool.Put(obj)
			}
		}()
	}
	wg.Wait()
}

// BenchmarkRefPool_NewObjectVsPool 新建对象vs池对象性能对比
func BenchmarkRefPool_NewObjectVsPool(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	b.Run("NewObject", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			obj := NewTestObject()
			_ = obj
		}
	})

	b.Run("PoolObject", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			obj := pool.Get()
			pool.Put(obj)
		}
	})
}

// BenchmarkRefPool_Clear Clear操作性能测试
func BenchmarkRefPool_Clear(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	// 预先填充池
	for i := 0; i < 1000; i++ {
		obj := pool.Get()
		pool.Put(obj)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pool.Clear()
		// 重新填充以进行下一次测试
		for j := 0; j < 100; j++ {
			obj := pool.Get()
			pool.Put(obj)
		}
	}
}

// BenchmarkRefPool_PreventDuplicatePut 防止重复放入的性能影响
func BenchmarkRefPool_PreventDuplicatePut(b *testing.B) {
	pool := NewRefPool(NewTestObject)

	obj := pool.Get()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		pool.Put(obj) // 尝试重复放入
	}
}
