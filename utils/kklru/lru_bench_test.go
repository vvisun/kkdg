package kklru

import (
	"math/rand"
	"sync"
	"testing"
)

// 运行方式：
// go test ./utils/kklru -run=^$ -bench=. -benchmem

func BenchmarkLRU_Get(b *testing.B) {
	lru := NewLRU[int, int](1000)
	// 预热缓存
	for i := 0; i < 1000; i++ {
		lru.Set(i, i*2)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lru.Get(i % 1000)
	}
}

func BenchmarkLRU_Set(b *testing.B) {
	lru := NewLRU[int, int](1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lru.Set(i, i*2)
	}
}

func BenchmarkLRU_Set_WithEviction(b *testing.B) {
	lru := NewLRU[int, int](100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lru.Set(i, i*2) // 会触发淘汰
	}
}

func BenchmarkLRU_Get_UpdateOrder(b *testing.B) {
	lru := NewLRU[int, int](1000)
	// 预热缓存
	for i := 0; i < 1000; i++ {
		lru.Set(i, i*2)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := i % 1000
		lru.Get(key) // 更新访问顺序
	}
}

func BenchmarkLRU_Delete(b *testing.B) {
	lru := NewLRU[int, int](1000)
	// 预热缓存
	for i := 0; i < 1000; i++ {
		lru.Set(i, i*2)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lru.Delete(i % 1000)
	}
}

func BenchmarkLRU_Contains(b *testing.B) {
	lru := NewLRU[int, int](1000)
	// 预热缓存
	for i := 0; i < 1000; i++ {
		lru.Set(i, i*2)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lru.Contains(i % 1000)
	}
}

func BenchmarkLRU_Peek(b *testing.B) {
	lru := NewLRU[int, int](1000)
	// 预热缓存
	for i := 0; i < 1000; i++ {
		lru.Set(i, i*2)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lru.Peek(i % 1000)
	}
}

func BenchmarkLRU_Mixed(b *testing.B) {
	lru := NewLRU[int, int](1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := i % 2000
		switch i % 4 {
		case 0:
			lru.Set(key, i)
		case 1:
			_, _ = lru.Get(key)
		case 2:
			_ = lru.Contains(key)
		case 3:
			_ = lru.Delete(key)
		}
	}
}

func BenchmarkLRU_Parallel_Get(b *testing.B) {
	lru := NewLRU[int, int](1000)
	// 预热缓存
	for i := 0; i < 1000; i++ {
		lru.Set(i, i*2)
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		for pb.Next() {
			key := rng.Intn(1000)
			_, _ = lru.Get(key)
		}
	})
}

func BenchmarkLRU_Parallel_Set(b *testing.B) {
	lru := NewLRU[int, int](1000)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		counter := 0
		for pb.Next() {
			lru.Set(counter, counter*2)
			counter++
		}
	})
}

func BenchmarkLRU_Parallel_Mixed(b *testing.B) {
	lru := NewLRU[int, int](1000)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewSource(42))
		counter := 0
		for pb.Next() {
			key := rng.Intn(2000)
			switch counter % 4 {
			case 0:
				lru.Set(key, counter)
			case 1:
				_, _ = lru.Get(key)
			case 2:
				_ = lru.Contains(key)
			case 3:
				_ = lru.Delete(key)
			}
			counter++
		}
	})
}

func BenchmarkLRU_SmallCapacity(b *testing.B) {
	lru := NewLRU[int, int](10)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		lru.Set(i, i*2) // 频繁淘汰
		_, _ = lru.Get(i % 100)
	}
}

func BenchmarkLRU_LargeCapacity(b *testing.B) {
	cap := 300000
	lru := NewLRU[int, int](cap)
	// 预热缓存
	for i := 0; i < cap; i++ {
		lru.Set(i, i*2)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = lru.Get(i % cap)
	}
}

func BenchmarkLRU_StringKey(b *testing.B) {
	lru := NewLRU[string, int](1000)
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = string(rune('a' + i%26))
		lru.Set(keys[i], i)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = lru.Get(keys[i%1000])
	}
}

func BenchmarkLRU_Clear(b *testing.B) {
	lru := NewLRU[int, int](1000)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// 填充缓存
		for j := 0; j < 1000; j++ {
			lru.Set(j, j*2)
		}
		// 清空
		lru.Clear()
	}
}

// 对比测试：模拟当前项目中的简单 map + evict 方式
func BenchmarkLRU_Comparison_SimpleMap(b *testing.B) {
	m := make(map[int]int, 1000)
	mu := sync.RWMutex{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		mu.Lock()
		m[i] = i * 2
		if len(m) > 1000 {
			// 简单删除一个（不是真正的 LRU）
			for k := range m {
				delete(m, k)
				break
			}
		}
		mu.Unlock()
	}
}
