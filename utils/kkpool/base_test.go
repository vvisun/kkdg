package kkpool

import (
	"sync/atomic"
	"testing"
)

// TestBasePoolObject 测试基础池对象
func TestBasePoolObject(t *testing.T) {
	obj := &BasePoolObject{}

	// 测试初始引用计数为0
	if atomic.LoadInt64(obj.getPoolRef()) != 0 {
		t.Errorf("初始引用计数应该为0，实际为%v", atomic.LoadInt64(obj.getPoolRef()))
	}

	// 测试OnDtor默认实现（不应该panic）
	obj.OnDtor()
}
