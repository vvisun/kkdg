package kkpool

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

// SfxPool 泛型对象池
type SfxPool[T any] struct {
	pool    sync.Pool
	size    int64 // 使用原子操作
	newFunc func() T
}

// NewFxPool 创建泛型对象池
func NewSfxPool[T any](newFunc func() T) *SfxPool[T] {
	return &SfxPool[T]{
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
func (fp *SfxPool[T]) Get() T {
	obj := fp.pool.Get().(T)

	if xreflect.IsNil(obj) || !xreflect.IsPointer(obj) || xreflect.IsDoublePointer(obj) {
		kklog.Errorf("obj is nil or not ptr")
		return obj
	}

	// 原子操作减少size
	if atomic.LoadInt64(&fp.size) > 0 {
		atomic.AddInt64(&fp.size, -1)
	}

	return obj
}

// Put 归还对象
func (fp *SfxPool[T]) Put(obj T) {
	// 检查对象是否为nil（对于指针类型）
	if xreflect.IsNil(obj) || !xreflect.IsPointer(obj) || xreflect.IsDoublePointer(obj) {
		kklog.Errorf("obj is nil or not ptr")
		return
	}
	fp.pool.Put(obj)
	atomic.AddInt64(&fp.size, 1)
}

// Clear 清空池
func (fp *SfxPool[T]) Clear() {
	newFunc := fp.newFunc
	fp.pool = sync.Pool{
		New: func() interface{} {
			return newFunc()
		},
	}
	atomic.StoreInt64(&fp.size, 0)
}

// Size 获取池大小（原子操作）
func (fp *SfxPool[T]) Size() int64 {
	return atomic.LoadInt64(&fp.size)
}

// GetStats 获取池统计信息
func (fp *SfxPool[T]) GetStats() PoolStats {
	return PoolStats{
		Size: fp.Size(),
	}
}
