package kkpool

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

// RefPoolObject 引用池对象接口
type RefPoolObject interface {
	OnDtor()            // 析构函数, 子类可以重写。回收对象时会自动调用
	getPoolRef() *int64 // 获取池引用计数，用于防止重复放入池中。0表示未在池中，大于0表示在池中
}

// RefPool 引用计数对象池。
// 线程安全+支持回收析构+引用计数（防止同一个对象被重复放入池中，导致取到重复对象）
type RefPool[T RefPoolObject] struct {
	pool    sync.Pool
	size    int64 // 使用原子操作
	newFunc func() T
}

// NewRefPool 创建引用计数对象池
func NewRefPool[T RefPoolObject](newFunc func() T) *RefPool[T] {
	return &RefPool[T]{
		pool: sync.Pool{
			New: func() interface{} {
				return newFunc()
			},
		},
		newFunc: newFunc,
		size:    0,
	}
}

// Get 获取对象
func (rp *RefPool[T]) Get() T {
	obj := rp.pool.Get().(T)

	if xreflect.IsNil(obj) || !xreflect.IsPointer(obj) || xreflect.IsDoublePointer(obj) {
		kklog.Debugf("obj is nil or not ptr")
		return obj
	}

	// 原子操作减少size
	if atomic.LoadInt64(&rp.size) > 0 {
		atomic.AddInt64(&rp.size, -1)
	}

	// 重置引用计数为0，表示对象已被取出
	atomic.StoreInt64(obj.getPoolRef(), 0)

	return obj
}

// Put 归还对象
func (rp *RefPool[T]) Put(obj T) {
	if atomic.LoadInt64(&rp.size) > 4096 {
		// pool size is too large, just skip it.
		return
	}

	// 检查对象是否为nil（对于指针类型）
	if xreflect.IsNil(obj) || !xreflect.IsPointer(obj) || xreflect.IsDoublePointer(obj) {
		kklog.Errorf("obj is nil or not ptr")
		return
	}

	// 检查引用计数，防止重复放入
	poolRef := obj.getPoolRef()
	if atomic.LoadInt64(poolRef) > 0 {
		return
	}

	// 设置引用计数为1，表示对象在池中
	atomic.StoreInt64(poolRef, 1)

	obj.OnDtor()
	rp.pool.Put(obj)
	atomic.AddInt64(&rp.size, 1)
}

// Clear 清空池
func (rp *RefPool[T]) Clear() {
	newFunc := rp.newFunc
	rp.pool = sync.Pool{
		New: func() interface{} {
			return newFunc()
		},
	}
	atomic.StoreInt64(&rp.size, 0)
}

// Size 获取池大小（原子操作）
func (rp *RefPool[T]) Size() int64 {
	return atomic.LoadInt64(&rp.size)
}

// GetStats 获取池统计信息
func (rp *RefPool[T]) GetStats() PoolStats {
	return PoolStats{
		Size: rp.Size(),
	}
}
