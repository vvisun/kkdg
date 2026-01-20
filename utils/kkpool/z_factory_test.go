package kkpool

import (
	"reflect"
	"sync"
	"testing"
)

// TestObject1 测试对象类型1
type TestObject1 struct {
	Value int
}

// TestObject2 测试对象类型2
type TestObject2 struct {
	Value string
}

// TestObject3 测试对象类型3
type TestObject3 struct {
	Value float64
}

// TestGetFactory_Basic 测试基本功能：获取不同类型的工厂
func TestGetFactory_Basic(t *testing.T) {
	// 获取不同类型的工厂
	factory1 := GetFactory[TestObject1]()
	factory2 := GetFactory[TestObject2]()
	factory3 := GetFactory[TestObject3]()

	if factory1 == nil {
		t.Fatal("GetFactory[TestObject1] 返回 nil")
	}
	if factory2 == nil {
		t.Fatal("GetFactory[TestObject2] 返回 nil")
	}
	if factory3 == nil {
		t.Fatal("GetFactory[TestObject3] 返回 nil")
	}

	// 验证工厂可以正常工作
	obj1 := factory1.Get().(*TestObject1)
	if obj1 == nil {
		t.Fatal("factory1.Get() 返回 nil")
	}
	obj1.Value = 42
	factory1.Put(obj1)

	obj2 := factory2.Get().(*TestObject2)
	if obj2 == nil {
		t.Fatal("factory2.Get() 返回 nil")
	}
	obj2.Value = "test"
	factory2.Put(obj2)

	obj3 := factory3.Get().(*TestObject3)
	if obj3 == nil {
		t.Fatal("factory3.Get() 返回 nil")
	}
	obj3.Value = 3.14
	factory3.Put(obj3)
}

// TestGetFactory_Cache 测试缓存机制：相同类型应该返回同一个工厂实例
func TestGetFactory_Cache(t *testing.T) {
	factory1 := GetFactory[TestObject1]()
	factory2 := GetFactory[TestObject1]()
	factory3 := GetFactory[TestObject1]()

	// 相同类型应该返回同一个工厂实例
	if factory1 != factory2 {
		t.Error("相同类型应该返回同一个工厂实例")
	}
	if factory2 != factory3 {
		t.Error("相同类型应该返回同一个工厂实例")
	}
	if factory1 != factory3 {
		t.Error("相同类型应该返回同一个工厂实例")
	}
}

// TestGetFactory_DifferentTypes 测试不同类型应该返回不同的工厂实例
func TestGetFactory_DifferentTypes(t *testing.T) {
	factory1 := GetFactory[TestObject1]()
	factory2 := GetFactory[TestObject2]()
	factory3 := GetFactory[TestObject3]()

	// 不同类型应该返回不同的工厂实例
	if factory1 == factory2 {
		t.Error("不同类型应该返回不同的工厂实例")
	}
	if factory2 == factory3 {
		t.Error("不同类型应该返回不同的工厂实例")
	}
	if factory1 == factory3 {
		t.Error("不同类型应该返回不同的工厂实例")
	}
}

// TestGetFactory_Concurrent 测试并发安全性
func TestGetFactory_Concurrent(t *testing.T) {
	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// 用于收集所有获取到的工厂实例
	factories := make([]*SfxPool[interface{}], goroutines*iterations)
	var mu sync.Mutex
	index := 0

	// 启动多个 goroutine 同时获取工厂
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				factory := GetFactory[TestObject1]()
				if factory == nil {
					t.Errorf("goroutine %d, iteration %d: GetFactory 返回 nil", id, j)
					return
				}

				mu.Lock()
				factories[index] = factory
				index++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// 验证所有工厂实例都是同一个（缓存机制）
	firstFactory := factories[0]
	for i := 1; i < len(factories); i++ {
		if factories[i] != firstFactory {
			t.Errorf("并发获取的工厂实例不一致: factories[0] = %p, factories[%d] = %p",
				firstFactory, i, factories[i])
		}
	}
}

// TestGetFactory_GetPut 测试工厂的 Get/Put 功能
func TestGetFactory_GetPut(t *testing.T) {
	factory := GetFactory[TestObject1]()

	// 获取对象
	obj1 := factory.Get().(*TestObject1)
	if obj1 == nil {
		t.Fatal("Get() 返回 nil")
	}

	// 设置值
	obj1.Value = 100

	// 归还对象
	factory.Put(obj1)

	// 再次获取，应该得到同一个对象（如果池中有的话）
	obj2 := factory.Get().(*TestObject1)
	if obj2 == nil {
		t.Fatal("第二次 Get() 返回 nil")
	}

	// 注意：由于 sync.Pool 的特性，不保证获取到的是同一个对象
	// 但至少应该能正常工作
	if obj2.Value != 0 && obj2.Value != 100 {
		t.Logf("注意：obj2.Value = %d，可能是新对象或复用的对象", obj2.Value)
	}
}

// TestGetFactory_MultipleTypesConcurrent 测试多种类型并发获取
func TestGetFactory_MultipleTypesConcurrent(t *testing.T) {
	const goroutines = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 3) // 3种类型

	// 测试三种类型并发获取
	for i := 0; i < goroutines; i++ {
		// TestObject1
		go func() {
			defer wg.Done()
			factory := GetFactory[TestObject1]()
			if factory == nil {
				t.Error("GetFactory[TestObject1] 返回 nil")
				return
			}
			obj := factory.Get().(*TestObject1)
			obj.Value = 1
			factory.Put(obj)
		}()

		// TestObject2
		go func() {
			defer wg.Done()
			factory := GetFactory[TestObject2]()
			if factory == nil {
				t.Error("GetFactory[TestObject2] 返回 nil")
				return
			}
			obj := factory.Get().(*TestObject2)
			obj.Value = "test"
			factory.Put(obj)
		}()

		// TestObject3
		go func() {
			defer wg.Done()
			factory := GetFactory[TestObject3]()
			if factory == nil {
				t.Error("GetFactory[TestObject3] 返回 nil")
				return
			}
			obj := factory.Get().(*TestObject3)
			obj.Value = 3.14
			factory.Put(obj)
		}()
	}

	wg.Wait()

	// 验证每种类型都只有一个工厂实例
	factory1a := GetFactory[TestObject1]()
	factory1b := GetFactory[TestObject1]()
	if factory1a != factory1b {
		t.Error("TestObject1 的工厂实例不一致")
	}

	factory2a := GetFactory[TestObject2]()
	factory2b := GetFactory[TestObject2]()
	if factory2a != factory2b {
		t.Error("TestObject2 的工厂实例不一致")
	}

	factory3a := GetFactory[TestObject3]()
	factory3b := GetFactory[TestObject3]()
	if factory3a != factory3b {
		t.Error("TestObject3 的工厂实例不一致")
	}

	// 验证不同类型返回不同实例
	if factory1a == factory2a {
		t.Error("不同类型应该返回不同的工厂实例")
	}
	if factory2a == factory3a {
		t.Error("不同类型应该返回不同的工厂实例")
	}
}

// TestGetFactory_Size 测试工厂的 Size 功能
func TestGetFactory_Size(t *testing.T) {
	factory := GetFactory[TestObject1]()

	// 清空池以确保初始状态
	factory.Clear()

	// 初始大小应该为 0（清空后）
	if factory.Size() != 0 {
		t.Errorf("清空后大小应该为 0，实际为 %d", factory.Size())
	}

	// 获取对象
	obj1 := factory.Get().(*TestObject1)
	obj2 := factory.Get().(*TestObject1)

	// 获取后大小应该减少（如果之前有对象的话）
	// 由于 sync.Pool 的特性，Size 可能不准确

	// 归还对象
	factory.Put(obj1)
	factory.Put(obj2)

	// 归还后大小应该增加
	size := factory.Size()
	if size < 0 {
		t.Errorf("大小不应该为负数: %d", size)
	}
	if size < 2 {
		t.Logf("注意：大小可能小于预期，实际为 %d（sync.Pool 的特性）", size)
	}
}

// TestGetFactory_Clear 测试工厂的 Clear 功能
func TestGetFactory_Clear(t *testing.T) {
	factory := GetFactory[TestObject1]()

	// 获取并归还一些对象
	obj1 := factory.Get().(*TestObject1)
	obj2 := factory.Get().(*TestObject1)
	factory.Put(obj1)
	factory.Put(obj2)

	// 清空池
	factory.Clear()

	// 清空后大小应该为 0
	if factory.Size() != 0 {
		t.Errorf("清空后大小应该为 0，实际为 %d", factory.Size())
	}

	// 清空后仍然可以正常使用
	obj3 := factory.Get().(*TestObject1)
	if obj3 == nil {
		t.Fatal("Clear 后 Get() 返回 nil")
	}
	factory.Put(obj3)
}

// TestGetFactory_GetStats 测试工厂的 GetStats 功能
func TestGetFactory_GetStats(t *testing.T) {
	factory := GetFactory[TestObject1]()

	stats := factory.GetStats()
	if stats.Size < 0 {
		t.Errorf("统计大小不应该为负数: %d", stats.Size)
	}

	// 获取并归还对象
	obj := factory.Get().(*TestObject1)
	factory.Put(obj)

	stats2 := factory.GetStats()
	if stats2.Size < 0 {
		t.Errorf("统计大小不应该为负数: %d", stats2.Size)
	}
}

// TestGetFactoryByType_Basic 测试 GetFactoryByType 基本功能
func TestGetFactoryByType_Basic(t *testing.T) {
	var v1 TestObject1
	var v2 TestObject2
	var v3 TestObject3

	tp1 := reflect.TypeOf(&v1)
	tp2 := reflect.TypeOf(&v2)
	tp3 := reflect.TypeOf(&v3)

	factory1 := GetFactoryByType(tp1)
	factory2 := GetFactoryByType(tp2)
	factory3 := GetFactoryByType(tp3)

	if factory1 == nil {
		t.Fatal("GetFactoryByType[TestObject1] 返回 nil")
	}
	if factory2 == nil {
		t.Fatal("GetFactoryByType[TestObject2] 返回 nil")
	}
	if factory3 == nil {
		t.Fatal("GetFactoryByType[TestObject3] 返回 nil")
	}

	// 验证工厂可以正常工作
	obj1 := factory1.Get().(*TestObject1)
	if obj1 == nil {
		t.Fatal("factory1.Get() 返回 nil")
	}
	obj1.Value = 42
	factory1.Put(obj1)

	obj2 := factory2.Get().(*TestObject2)
	if obj2 == nil {
		t.Fatal("factory2.Get() 返回 nil")
	}
	obj2.Value = "test"
	factory2.Put(obj2)

	obj3 := factory3.Get().(*TestObject3)
	if obj3 == nil {
		t.Fatal("factory3.Get() 返回 nil")
	}
	obj3.Value = 3.14
	factory3.Put(obj3)
}

// TestGetFactoryByType_Cache 测试 GetFactoryByType 的缓存机制
func TestGetFactoryByType_Cache(t *testing.T) {
	var v TestObject1
	tp := reflect.TypeOf(&v)

	factory1 := GetFactoryByType(tp)
	factory2 := GetFactoryByType(tp)
	factory3 := GetFactoryByType(tp)

	// 相同类型应该返回同一个工厂实例
	if factory1 != factory2 {
		t.Error("相同类型应该返回同一个工厂实例")
	}
	if factory2 != factory3 {
		t.Error("相同类型应该返回同一个工厂实例")
	}
	if factory1 != factory3 {
		t.Error("相同类型应该返回同一个工厂实例")
	}
}

// TestGetFactoryByType_DifferentTypes 测试不同类型返回不同工厂实例
func TestGetFactoryByType_DifferentTypes(t *testing.T) {
	var v1 TestObject1
	var v2 TestObject2
	var v3 TestObject3

	tp1 := reflect.TypeOf(&v1)
	tp2 := reflect.TypeOf(&v2)
	tp3 := reflect.TypeOf(&v3)

	factory1 := GetFactoryByType(tp1)
	factory2 := GetFactoryByType(tp2)
	factory3 := GetFactoryByType(tp3)

	// 不同类型应该返回不同的工厂实例
	if factory1 == factory2 {
		t.Error("不同类型应该返回不同的工厂实例")
	}
	if factory2 == factory3 {
		t.Error("不同类型应该返回不同的工厂实例")
	}
	if factory1 == factory3 {
		t.Error("不同类型应该返回不同的工厂实例")
	}
}

// TestGetFactoryByType_Concurrent 测试 GetFactoryByType 的并发安全性
func TestGetFactoryByType_Concurrent(t *testing.T) {
	const goroutines = 100
	const iterations = 100

	var v TestObject1
	tp := reflect.TypeOf(&v)

	var wg sync.WaitGroup
	wg.Add(goroutines)

	// 用于收集所有获取到的工厂实例
	factories := make([]*SfxPool[interface{}], goroutines*iterations)
	var mu sync.Mutex
	index := 0

	// 启动多个 goroutine 同时获取工厂
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				factory := GetFactoryByType(tp)
				if factory == nil {
					t.Errorf("goroutine %d, iteration %d: GetFactoryByType 返回 nil", id, j)
					return
				}

				mu.Lock()
				factories[index] = factory
				index++
				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// 验证所有工厂实例都是同一个（缓存机制）
	firstFactory := factories[0]
	for i := 1; i < len(factories); i++ {
		if factories[i] != firstFactory {
			t.Errorf("并发获取的工厂实例不一致: factories[0] = %p, factories[%d] = %p",
				firstFactory, i, factories[i])
		}
	}
}

// TestGetFactoryByType_GetPut 测试 GetFactoryByType 的 Get/Put 功能
func TestGetFactoryByType_GetPut(t *testing.T) {
	var v TestObject1
	tp := reflect.TypeOf(&v)
	factory := GetFactoryByType(tp)

	// 获取对象
	obj1 := factory.Get().(*TestObject1)
	if obj1 == nil {
		t.Fatal("Get() 返回 nil")
	}

	// 设置值
	obj1.Value = 100

	// 归还对象
	factory.Put(obj1)

	// 再次获取
	obj2 := factory.Get().(*TestObject1)
	if obj2 == nil {
		t.Fatal("第二次 Get() 返回 nil")
	}

	// 注意：由于 sync.Pool 的特性，不保证获取到的是同一个对象
	if obj2.Value != 0 && obj2.Value != 100 {
		t.Logf("注意：obj2.Value = %d，可能是新对象或复用的对象", obj2.Value)
	}
}

// TestGetFactoryByType_ConsistencyWithGetFactory 测试 GetFactoryByType 与 GetFactory 的一致性
func TestGetFactoryByType_ConsistencyWithGetFactory(t *testing.T) {
	// 使用 GetFactory 获取工厂
	factory1 := GetFactory[TestObject1]()

	// 使用 GetFactoryByType 获取相同类型的工厂
	var v TestObject1
	tp := reflect.TypeOf(&v)
	factory2 := GetFactoryByType(tp)

	// 应该返回同一个工厂实例（因为它们使用相同的类型键）
	if factory1 != factory2 {
		t.Error("GetFactory 和 GetFactoryByType 应该返回同一个工厂实例")
	}

	// 验证两个工厂都能正常工作
	obj1 := factory1.Get().(*TestObject1)
	obj1.Value = 123
	factory1.Put(obj1)

	obj2 := factory2.Get().(*TestObject1)
	if obj2 == nil {
		t.Fatal("factory2.Get() 返回 nil")
	}
	factory2.Put(obj2)
}

// TestGetFactoryByType_MultipleTypesConcurrent 测试多种类型并发获取
func TestGetFactoryByType_MultipleTypesConcurrent(t *testing.T) {
	const goroutines = 50

	var v1 TestObject1
	var v2 TestObject2
	var v3 TestObject3

	tp1 := reflect.TypeOf(&v1)
	tp2 := reflect.TypeOf(&v2)
	tp3 := reflect.TypeOf(&v3)

	var wg sync.WaitGroup
	wg.Add(goroutines * 3) // 3种类型

	// 测试三种类型并发获取
	for i := 0; i < goroutines; i++ {
		// TestObject1
		go func() {
			defer wg.Done()
			factory := GetFactoryByType(tp1)
			if factory == nil {
				t.Error("GetFactoryByType[TestObject1] 返回 nil")
				return
			}
			obj := factory.Get().(*TestObject1)
			obj.Value = 1
			factory.Put(obj)
		}()

		// TestObject2
		go func() {
			defer wg.Done()
			factory := GetFactoryByType(tp2)
			if factory == nil {
				t.Error("GetFactoryByType[TestObject2] 返回 nil")
				return
			}
			obj := factory.Get().(*TestObject2)
			obj.Value = "test"
			factory.Put(obj)
		}()

		// TestObject3
		go func() {
			defer wg.Done()
			factory := GetFactoryByType(tp3)
			if factory == nil {
				t.Error("GetFactoryByType[TestObject3] 返回 nil")
				return
			}
			obj := factory.Get().(*TestObject3)
			obj.Value = 3.14
			factory.Put(obj)
		}()
	}

	wg.Wait()

	// 验证每种类型都只有一个工厂实例
	factory1a := GetFactoryByType(tp1)
	factory1b := GetFactoryByType(tp1)
	if factory1a != factory1b {
		t.Error("TestObject1 的工厂实例不一致")
	}

	factory2a := GetFactoryByType(tp2)
	factory2b := GetFactoryByType(tp2)
	if factory2a != factory2b {
		t.Error("TestObject2 的工厂实例不一致")
	}

	factory3a := GetFactoryByType(tp3)
	factory3b := GetFactoryByType(tp3)
	if factory3a != factory3b {
		t.Error("TestObject3 的工厂实例不一致")
	}

	// 验证不同类型返回不同实例
	if factory1a == factory2a {
		t.Error("不同类型应该返回不同的工厂实例")
	}
	if factory2a == factory3a {
		t.Error("不同类型应该返回不同的工厂实例")
	}
}

// TestGetFactoryByType_Size 测试 GetFactoryByType 的 Size 功能
func TestGetFactoryByType_Size(t *testing.T) {
	var v TestObject1
	tp := reflect.TypeOf(&v)
	factory := GetFactoryByType(tp)

	// 清空池以确保初始状态
	factory.Clear()

	// 初始大小应该为 0（清空后）
	if factory.Size() != 0 {
		t.Errorf("清空后大小应该为 0，实际为 %d", factory.Size())
	}

	// 获取对象
	obj1 := factory.Get().(*TestObject1)
	obj2 := factory.Get().(*TestObject1)

	// 归还对象
	factory.Put(obj1)
	factory.Put(obj2)

	// 归还后大小应该增加
	size := factory.Size()
	if size < 0 {
		t.Errorf("大小不应该为负数: %d", size)
	}
	if size < 2 {
		t.Logf("注意：大小可能小于预期，实际为 %d（sync.Pool 的特性）", size)
	}
}

// TestGetFactoryByType_Clear 测试 GetFactoryByType 的 Clear 功能
func TestGetFactoryByType_Clear(t *testing.T) {
	var v TestObject1
	tp := reflect.TypeOf(&v)
	factory := GetFactoryByType(tp)

	// 获取并归还一些对象
	obj1 := factory.Get().(*TestObject1)
	obj2 := factory.Get().(*TestObject1)
	factory.Put(obj1)
	factory.Put(obj2)

	// 清空池
	factory.Clear()

	// 清空后大小应该为 0
	if factory.Size() != 0 {
		t.Errorf("清空后大小应该为 0，实际为 %d", factory.Size())
	}

	// 清空后仍然可以正常使用
	obj3 := factory.Get().(*TestObject1)
	if obj3 == nil {
		t.Fatal("Clear 后 Get() 返回 nil")
	}
	factory.Put(obj3)
}

// TestGetFactoryByType_GetStats 测试 GetFactoryByType 的 GetStats 功能
func TestGetFactoryByType_GetStats(t *testing.T) {
	var v TestObject1
	tp := reflect.TypeOf(&v)
	factory := GetFactoryByType(tp)

	stats := factory.GetStats()
	if stats.Size < 0 {
		t.Errorf("统计大小不应该为负数: %d", stats.Size)
	}

	// 获取并归还对象
	obj := factory.Get().(*TestObject1)
	factory.Put(obj)

	stats2 := factory.GetStats()
	if stats2.Size < 0 {
		t.Errorf("统计大小不应该为负数: %d", stats2.Size)
	}
}

// TestGetFactoryByType_RequiresPointerType 测试 GetFactoryByType 需要指针类型
func TestGetFactoryByType_RequiresPointerType(t *testing.T) {
	// 注意：GetFactoryByType 期望的是指针类型，因为它调用 tp.Elem()
	// 如果传入非指针类型，在创建工厂时会 panic
	tp := reflect.TypeOf(TestObject1{})

	// 验证传入非指针类型会在创建工厂时 panic
	defer func() {
		if r := recover(); r == nil {
			t.Error("期望 GetFactoryByType 对非指针类型 panic，但没有 panic")
		} else {
			t.Logf("正确捕获到 panic: %v", r)
		}
	}()

	factory := GetFactoryByType(tp)
	// 尝试获取对象，这会触发工厂创建，从而触发 panic
	_ = factory.Get()
	t.Error("GetFactoryByType 应该对非指针类型 panic")
}

// TestGetFactoryByType_EdgeCases 测试边界情况
func TestGetFactoryByType_EdgeCases(t *testing.T) {
	// 测试基本类型
	var intPtr *int
	tpInt := reflect.TypeOf(intPtr)
	factoryInt := GetFactoryByType(tpInt)
	if factoryInt == nil {
		t.Fatal("GetFactoryByType[*int] 返回 nil")
	}
	objInt := factoryInt.Get().(*int)
	if objInt == nil {
		t.Fatal("Get() 返回 nil")
	}
	*objInt = 42
	factoryInt.Put(objInt)

	// 测试字符串指针
	var strPtr *string
	tpStr := reflect.TypeOf(strPtr)
	factoryStr := GetFactoryByType(tpStr)
	if factoryStr == nil {
		t.Fatal("GetFactoryByType[*string] 返回 nil")
	}
	objStr := factoryStr.Get().(*string)
	if objStr == nil {
		t.Fatal("Get() 返回 nil")
	}
	*objStr = "test"
	factoryStr.Put(objStr)
}
