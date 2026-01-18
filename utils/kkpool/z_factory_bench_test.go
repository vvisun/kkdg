package kkpool

import (
	"sync"
	"testing"
)

// BenchmarkGetFactory_FirstCall 测试首次获取工厂的性能
func BenchmarkGetFactory_FirstCall(b *testing.B) {
	// 使用不同的类型确保每次都是首次调用
	type BenchObj1 struct{ Value int }
	type BenchObj2 struct{ Value int }
	type BenchObj3 struct{ Value int }

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// 每次使用不同的类型，模拟首次调用
		switch i % 3 {
		case 0:
			_ = GetFactory[BenchObj1]()
		case 1:
			_ = GetFactory[BenchObj2]()
		case 2:
			_ = GetFactory[BenchObj3]()
		}
	}
}

// BenchmarkGetFactory_Cached 测试缓存命中时的性能
func BenchmarkGetFactory_Cached(b *testing.B) {
	type BenchObj struct{ Value int }

	// 预先获取一次，确保缓存
	_ = GetFactory[BenchObj]()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = GetFactory[BenchObj]()
	}
}

// BenchmarkGetFactory_Concurrent 并发获取工厂的性能
func BenchmarkGetFactory_Concurrent(b *testing.B) {
	type BenchObj struct{ Value int }

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = GetFactory[BenchObj]()
		}
	})
}

// BenchmarkGetFactory_Concurrent_MixedTypes 并发获取不同类型工厂的性能
func BenchmarkGetFactory_Concurrent_MixedTypes(b *testing.B) {
	type BenchObj1 struct{ Value int }
	type BenchObj2 struct{ Value string }
	type BenchObj3 struct{ Value float64 }

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			switch i % 3 {
			case 0:
				_ = GetFactory[BenchObj1]()
			case 1:
				_ = GetFactory[BenchObj2]()
			case 2:
				_ = GetFactory[BenchObj3]()
			}
			i++
		}
	})
}

// BenchmarkFactory_GetPut 通过工厂获取/归还对象的性能
func BenchmarkFactory_GetPut(b *testing.B) {
	type BenchObj struct{ Value int }

	factory := GetFactory[BenchObj]()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		obj := factory.Get().(*BenchObj)
		factory.Put(obj)
	}
}

// BenchmarkFactory_GetOnly 只测试获取对象的性能
func BenchmarkFactory_GetOnly(b *testing.B) {
	type BenchObj struct{ Value int }

	factory := GetFactory[BenchObj]()

	// 预先放入一些对象
	for i := 0; i < 1000; i++ {
		obj := factory.Get().(*BenchObj)
		factory.Put(obj)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		obj := factory.Get().(*BenchObj)
		_ = obj
		// 不归还，测试只获取的性能
	}
}

// BenchmarkFactory_Concurrent_GetPut 并发获取/归还对象的性能
func BenchmarkFactory_Concurrent_GetPut(b *testing.B) {
	type BenchObj struct{ Value int }

	factory := GetFactory[BenchObj]()

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			obj := factory.Get().(*BenchObj)
			factory.Put(obj)
		}
	})
}

// BenchmarkFactory_NewObjectVsPool 新建对象vs工厂对象池性能对比
func BenchmarkFactory_NewObjectVsPool(b *testing.B) {
	type BenchObj struct{ Value int }

	factory := GetFactory[BenchObj]()

	b.Run("NewObject", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			obj := new(BenchObj)
			_ = obj
		}
	})

	b.Run("FactoryPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			obj := factory.Get().(*BenchObj)
			factory.Put(obj)
		}
	})
}

// BenchmarkFactory_DirectSfxPoolVsFactory 直接使用SfxPool vs 通过Factory的性能对比
func BenchmarkFactory_DirectSfxPoolVsFactory(b *testing.B) {
	type BenchObj struct{ Value int }

	directPool := NewSfxPool(func() *BenchObj {
		return new(BenchObj)
	})
	factory := GetFactory[BenchObj]()

	b.Run("DirectSfxPool", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			obj := directPool.Get()
			directPool.Put(obj)
		}
	})

	b.Run("Factory", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			obj := factory.Get().(*BenchObj)
			factory.Put(obj)
		}
	})
}

// BenchmarkFactory_HighContention 高争用场景下的性能
func BenchmarkFactory_HighContention(b *testing.B) {
	type BenchObj struct{ Value int }

	factory := GetFactory[BenchObj]()

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
				obj := factory.Get().(*BenchObj)
				// 模拟一些处理时间
				obj.Value = i
				factory.Put(obj)
			}
		}()
	}
	wg.Wait()
}

// BenchmarkFactory_Size 获取工厂大小的性能
func BenchmarkFactory_Size(b *testing.B) {
	type BenchObj struct{ Value int }

	factory := GetFactory[BenchObj]()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = factory.Size()
	}
}

// BenchmarkFactory_GetStats 获取工厂统计信息的性能
func BenchmarkFactory_GetStats(b *testing.B) {
	type BenchObj struct{ Value int }

	factory := GetFactory[BenchObj]()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = factory.GetStats()
	}
}

// BenchmarkFactory_Clear 清空工厂的性能
func BenchmarkFactory_Clear(b *testing.B) {
	type BenchObj struct{ Value int }

	factory := GetFactory[BenchObj]()

	// 预先填充池
	for i := 0; i < 1000; i++ {
		obj := factory.Get().(*BenchObj)
		factory.Put(obj)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		factory.Clear()
		// 重新填充以进行下一次测试
		for j := 0; j < 100; j++ {
			obj := factory.Get().(*BenchObj)
			factory.Put(obj)
		}
	}
}

// BenchmarkFactory_MultipleTypes 多种类型工厂的性能
func BenchmarkFactory_MultipleTypes(b *testing.B) {
	type BenchObj1 struct{ Value int }
	type BenchObj2 struct{ Value string }
	type BenchObj3 struct{ Value float64 }
	type BenchObj4 struct{ Value bool }
	type BenchObj5 struct{ Value []byte }

	factories := []*SfxPool[interface{}]{
		GetFactory[BenchObj1](),
		GetFactory[BenchObj2](),
		GetFactory[BenchObj3](),
		GetFactory[BenchObj4](),
		GetFactory[BenchObj5](),
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		factory := factories[i%len(factories)]
		obj := factory.Get()
		factory.Put(obj)
	}
}

// BenchmarkFactory_TypeAssertion 类型断言的开销
func BenchmarkFactory_TypeAssertion(b *testing.B) {
	type BenchObj struct{ Value int }

	factory := GetFactory[BenchObj]()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		obj := factory.Get().(*BenchObj)
		_ = obj.Value
		factory.Put(obj)
	}
}

// BenchmarkFactory_ReflectionOverhead 反射开销测试
func BenchmarkFactory_ReflectionOverhead(b *testing.B) {
	type BenchObj struct{ Value int }

	b.Run("WithReflection", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = GetFactory[BenchObj]()
		}
	})

	b.Run("DirectPool", func(b *testing.B) {
		pool := NewSfxPool(func() *BenchObj {
			return new(BenchObj)
		})
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_ = pool
		}
	})
}
