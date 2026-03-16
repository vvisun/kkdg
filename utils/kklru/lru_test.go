package kklru

import (
	"testing"
)

func TestLRU_Basic(t *testing.T) {
	lru := NewLRU[string, int](3)

	// 测试 Set 和 Get
	lru.Set("a", 1)
	lru.Set("b", 2)
	lru.Set("c", 3)

	if val, ok := lru.Get("a"); !ok || val != 1 {
		t.Errorf("Get('a') = %v, %v, want 1, true", val, ok)
	}
	if val, ok := lru.Get("b"); !ok || val != 2 {
		t.Errorf("Get('b') = %v, %v, want 2, true", val, ok)
	}
	if val, ok := lru.Get("c"); !ok || val != 3 {
		t.Errorf("Get('c') = %v, %v, want 3, true", val, ok)
	}

	if lru.Size() != 3 {
		t.Errorf("Size() = %d, want 3", lru.Size())
	}
}

func TestLRU_Eviction(t *testing.T) {
	lru := NewLRU[string, int](3)

	// 添加 3 个元素
	lru.Set("a", 1)
	lru.Set("b", 2)
	lru.Set("c", 3)

	// 添加第 4 个元素，应该淘汰 "a"（最久未使用）
	lru.Set("d", 4)

	if _, ok := lru.Get("a"); ok {
		t.Errorf("Get('a') should return false, key should be evicted")
	}
	if val, ok := lru.Get("d"); !ok || val != 4 {
		t.Errorf("Get('d') = %v, %v, want 4, true", val, ok)
	}
	if lru.Size() != 3 {
		t.Errorf("Size() = %d, want 3", lru.Size())
	}
}

func TestLRU_Update(t *testing.T) {
	lru := NewLRU[string, int](3)

	lru.Set("a", 1)
	lru.Set("b", 2)
	lru.Set("a", 10) // 更新 "a" 的值

	if val, ok := lru.Get("a"); !ok || val != 10 {
		t.Errorf("Get('a') = %v, %v, want 10, true", val, ok)
	}
	if lru.Size() != 2 {
		t.Errorf("Size() = %d, want 2", lru.Size())
	}
}

func TestLRU_AccessOrder(t *testing.T) {
	lru := NewLRU[string, int](3)

	lru.Set("a", 1)
	lru.Set("b", 2)
	lru.Set("c", 3)

	// 访问 "a"，使其成为最近使用的
	lru.Get("a")

	// 添加新元素，应该淘汰 "b"（最久未使用），而不是 "a"
	lru.Set("d", 4)

	if _, ok := lru.Get("b"); ok {
		t.Errorf("Get('b') should return false, key should be evicted")
	}
	if _, ok := lru.Get("a"); !ok {
		t.Errorf("Get('a') should return true, key should not be evicted")
	}
}

func TestLRU_Delete(t *testing.T) {
	lru := NewLRU[string, int](3)

	lru.Set("a", 1)
	lru.Set("b", 2)
	lru.Set("c", 3)

	if !lru.Delete("b") {
		t.Errorf("Delete('b') should return true")
	}
	if lru.Delete("x") {
		t.Errorf("Delete('x') should return false")
	}

	if lru.Size() != 2 {
		t.Errorf("Size() = %d, want 2", lru.Size())
	}
	if _, ok := lru.Get("b"); ok {
		t.Errorf("Get('b') should return false after deletion")
	}
}

func TestLRU_Clear(t *testing.T) {
	lru := NewLRU[string, int](3)

	lru.Set("a", 1)
	lru.Set("b", 2)
	lru.Set("c", 3)

	lru.Clear()

	if lru.Size() != 0 {
		t.Errorf("Size() = %d, want 0", lru.Size())
	}
	if _, ok := lru.Get("a"); ok {
		t.Errorf("Get('a') should return false after Clear")
	}
}

func TestLRU_Contains(t *testing.T) {
	lru := NewLRU[string, int](3)

	lru.Set("a", 1)
	lru.Set("b", 2)

	if !lru.Contains("a") {
		t.Errorf("Contains('a') should return true")
	}
	if lru.Contains("c") {
		t.Errorf("Contains('c') should return false")
	}
}

func TestLRU_Peek(t *testing.T) {
	lru := NewLRU[string, int](3)

	lru.Set("a", 1)
	lru.Set("b", 2)
	lru.Set("c", 3)

	// Peek 不应该改变访问顺序
	// 当前顺序应该是 c -> b -> a（c 最近使用，a 最久未使用）
	val, ok := lru.Peek("a")
	if !ok || val != 1 {
		t.Errorf("Peek('a') = %v, %v, want 1, true", val, ok)
	}

	// 访问 "b" 使其成为最近使用的
	// 顺序变为 b -> c -> a（b 最近使用，a 最久未使用）
	lru.Get("b")

	// 添加新元素，应该淘汰 "a"（最久未使用），而不是 "c"
	// 因为 Peek 没有更新 "a" 的访问时间，所以 a 仍然是最久未使用的
	lru.Set("d", 4)

	// a 应该被淘汰（最久未使用）
	if _, ok := lru.Get("a"); ok {
		t.Errorf("Get('a') should return false, key should be evicted")
	}
	// c 应该还在（不是最久未使用的）
	if _, ok := lru.Get("c"); !ok {
		t.Errorf("Get('c') should return true, key should not be evicted")
	}
	// d 应该存在
	if _, ok := lru.Get("d"); !ok {
		t.Errorf("Get('d') should return true, key should exist")
	}
}

func TestLRU_Concurrent(t *testing.T) {
	lru := NewLRU[int, int](100)
	done := make(chan bool)

	// 并发写入
	go func() {
		for i := 0; i < 1000; i++ {
			lru.Set(i, i*2)
		}
		done <- true
	}()

	// 并发读取
	go func() {
		for i := 0; i < 1000; i++ {
			lru.Get(i)
		}
		done <- true
	}()

	// 并发删除
	go func() {
		for i := 0; i < 1000; i++ {
			lru.Delete(i)
		}
		done <- true
	}()

	// 等待所有 goroutine 完成
	<-done
	<-done
	<-done

	// 验证最终状态
	if lru.Size() > lru.Capacity() {
		t.Errorf("Size() = %d, should not exceed capacity %d", lru.Size(), lru.Capacity())
	}
}

func TestLRU_ZeroCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewLRU(0) should panic")
		}
	}()
	NewLRU[string, int](0)
}

func TestLRU_NegativeCapacity(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("NewLRU(-1) should panic")
		}
	}()
	NewLRU[string, int](-1)
}
