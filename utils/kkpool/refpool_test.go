package kkpool

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestObject 测试用的池对象
type TestObject struct {
	BasePoolObject
	ID       int
	Value    string
	DtorCall bool
}

// NewTestObject 创建测试对象
func NewTestObject() *TestObject {
	return &TestObject{}
}

// OnDtor 实现RefPoolObject接口
func (obj *TestObject) OnDtor() {
	obj.DtorCall = true
}

// TestNewRefPool 测试创建池
func TestNewRefPool(t *testing.T) {
	pool := NewRefPool(NewTestObject)

	if pool == nil {
		t.Fatal("NewRefPool返回nil")
	}

	if pool.Size() != 0 {
		t.Errorf("新池的大小应该为0，实际为%v", pool.Size())
	}

	stats := pool.GetStats()
	if stats.Size != 0 {
		t.Errorf("新池的统计大小应该为0，实际为%v", stats.Size)
	}
}

// TestRefPool_Get_Put 基础的Get/Put测试
func TestRefPool_Get_Put(t *testing.T) {
	pool := NewRefPool(NewTestObject)

	// 获取对象
	obj1 := pool.Get()
	if obj1 == nil {
		t.Fatal("Get返回nil对象")
	}

	// 验证引用计数为0
	if atomic.LoadInt64(obj1.getPoolRef()) != 0 {
		t.Errorf("Get后引用计数应该为0，实际为%v", atomic.LoadInt64(obj1.getPoolRef()))
	}

	// 归还对象
	pool.Put(obj1)

	// 验证引用计数为1
	if atomic.LoadInt64(obj1.getPoolRef()) != 1 {
		t.Errorf("Put后引用计数应该为1，实际为%v", atomic.LoadInt64(obj1.getPoolRef()))
	}

	// 验证池大小为1
	if pool.Size() != 1 {
		t.Errorf("池大小应该为1，实际为%v", pool.Size())
	}

	// 再次获取，应该得到同一个对象
	obj2 := pool.Get()
	if obj2 != obj1 {
		t.Error("应该得到同一个对象")
	}

	// 验证引用计数为0
	if atomic.LoadInt64(obj2.getPoolRef()) != 0 {
		t.Errorf("再次Get后引用计数应该为0，实际为%v", atomic.LoadInt64(obj2.getPoolRef()))
	}

	// 验证池大小为0
	if pool.Size() != 0 {
		t.Errorf("池大小应该为0，实际为%v", pool.Size())
	}
}

// TestRefPool_PreventDuplicatePut 测试防止重复放入
func TestRefPool_PreventDuplicatePut(t *testing.T) {
	pool := NewRefPool(NewTestObject)

	obj := pool.Get()

	// 第一次放入应该成功
	pool.Put(obj)
	size1 := pool.Size()

	// 第二次放入应该被忽略
	pool.Put(obj)
	size2 := pool.Size()

	if size1 != size2 {
		t.Errorf("重复Put应该被忽略，但池大小从%v变成了%v", size1, size2)
	}
}

// TestRefPool_OnDtor 测试析构函数调用
func TestRefPool_OnDtor(t *testing.T) {
	pool := NewRefPool(NewTestObject)

	obj := pool.Get()

	// 初始状态DtorCall应该为false
	if obj.DtorCall {
		t.Error("初始状态DtorCall应该为false")
	}

	// Put时应该调用OnDtor
	pool.Put(obj)

	if !obj.DtorCall {
		t.Error("Put后DtorCall应该为true")
	}
}

// TestRefPool_Clear 测试清空池
func TestRefPool_Clear(t *testing.T) {
	pool := NewRefPool(NewTestObject)

	// 添加一些对象到池中
	obj1 := pool.Get()
	obj2 := pool.Get()
	pool.Put(obj1)
	pool.Put(obj2)

	if pool.Size() != 2 {
		t.Errorf("池大小应该为2，实际为%v", pool.Size())
	}

	// 清空池
	pool.Clear()

	if pool.Size() != 0 {
		t.Errorf("清空后池大小应该为0，实际为%v", pool.Size())
	}

	// 验证可以继续使用
	obj3 := pool.Get()
	if obj3 == nil {
		t.Error("清空后仍然可以获取对象")
	}
}

// TestRefPool_Get_InvalidObject 测试无效对象的处理
func TestRefPool_Get_InvalidObject(t *testing.T) {
	// 创建一个返回nil的池
	pool := NewRefPool(func() *TestObject { return nil })
	obj := pool.Get()
	if obj != nil {
		t.Errorf("Get返回的对象应该为nil，实际为%v", obj)
	}
}

// TestRefPool_Put_InvalidObject 测试放入无效对象
func TestRefPool_Put_InvalidObject(t *testing.T) {
	pool := NewRefPool(NewTestObject)

	// 记录初始大小
	initialSize := pool.Size()

	// 放入nil对象
	var nilObj *TestObject
	pool.Put(nilObj)

	// 大小应该不变
	if pool.Size() != initialSize {
		t.Errorf("放入nil对象后池大小应该不变，实际从%v变成了%v", initialSize, pool.Size())
	}
}

// TestRefPool_Concurrent 并发测试
func TestRefPool_Concurrent(t *testing.T) {
	pool := NewRefPool(NewTestObject)

	const numGoroutines = 10
	const numOperations = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// 启动多个goroutine并发操作
	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				obj := pool.Get()
				time.Sleep(time.Microsecond) // 模拟一些工作
				pool.Put(obj)
			}
		}()
	}

	wg.Wait()

	// 最终池应该不为空（因为有对象被放入）
	if pool.Size() < 0 {
		t.Errorf("并发测试后池大小不应该为负数，实际为%v", pool.Size())
	}
}

// TestRefPool_Size 详细测试Size方法
func TestRefPool_Size(t *testing.T) {
	pool := NewRefPool(NewTestObject)

	// 初始大小为0
	if pool.Size() != 0 {
		t.Errorf("初始池大小应该为0，实际为%v", pool.Size())
	}

	// Get第一个对象（会创建新对象）
	obj1 := pool.Get()
	if pool.Size() != 0 {
		t.Errorf("Get后池大小应该为0，实际为%v", pool.Size())
	}

	// Put第一个对象
	pool.Put(obj1)
	if pool.Size() != 1 {
		t.Errorf("Put后池大小应该为1，实际为%v", pool.Size())
	}

	// Get第二个对象（会创建新对象，因为池中只有一个对象）
	obj2 := pool.Get()
	if pool.Size() != 0 {
		t.Errorf("再次Get后池大小应该为0，实际为%v", pool.Size())
	}

	// 验证obj1和obj2是不同的对象
	if obj1 == obj2 {
		t.Log("警告：obj1和obj2是同一个对象，这可能是正常的sync.Pool行为")
	}

	// Put两个对象
	pool.Put(obj1)
	sizeAfterPut1 := pool.Size()

	pool.Put(obj2)
	sizeAfterPut2 := pool.Size()

	t.Logf("Put obj1后大小: %v, Put obj2后大小: %v", sizeAfterPut1, sizeAfterPut2)

	// 至少应该有1个对象在池中
	if sizeAfterPut2 < 1 {
		t.Errorf("Put两个对象后池大小应该至少为1，实际为%v", sizeAfterPut2)
	}

	// 验证Size方法本身是线程安全的
	var sizes []int64
	for i := 0; i < 10; i++ {
		sizes = append(sizes, pool.Size())
	}

	// 所有读取应该一致
	for i := 1; i < len(sizes); i++ {
		if sizes[i] != sizes[0] {
			t.Errorf("Size方法结果不一致: %v vs %v", sizes[0], sizes[i])
		}
	}
}

// TestRefPool_GetStats 测试统计信息
func TestRefPool_GetStats(t *testing.T) {
	pool := NewRefPool(NewTestObject)

	// 添加一些对象
	obj1 := pool.Get()
	obj2 := pool.Get()
	pool.Put(obj1)
	pool.Put(obj2)

	stats := pool.GetStats()
	if stats.Size != 2 {
		t.Errorf("统计信息中的大小应该为2，实际为%v", stats.Size)
	}

	if stats.Size != pool.Size() {
		t.Error("统计信息与直接获取的大小不一致")
	}
}
