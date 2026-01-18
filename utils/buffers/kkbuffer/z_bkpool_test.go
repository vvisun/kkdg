package kkbuffer

import (
	"sync"
	"testing"
)

// TestNewBKPool 测试创建 bkPool
func TestNewBKPool(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)
	if pool == nil {
		t.Fatal("NewBKPool returned nil")
	}
	if pool.minSize != 64 {
		t.Errorf("minSize: got %d, want 64", pool.minSize)
	}
	if pool.maxSize != 1024 {
		t.Errorf("maxSize: got %d, want 1024", pool.maxSize)
	}
	if pool.steps != 4 {
		t.Errorf("steps: got %d, want 4", pool.steps)
	}
	if len(pool.poolList) != 4 {
		t.Errorf("poolList length: got %d, want 4", len(pool.poolList))
	}
	if len(pool.sizeList) != 4 {
		t.Errorf("sizeList length: got %d, want 4", len(pool.sizeList))
	}

	// 验证 sizeList 的值
	expectedSizes := []int{64, 304, 544, 1024} // (1024-64)/4 = 240, 所以是 64, 304, 544, 1024
	for i, expected := range expectedSizes {
		if pool.sizeList[i] != expected {
			t.Errorf("sizeList[%d]: got %d, want %d", i, expected, pool.sizeList[i])
		}
	}
}

// TestBKPool_GetWithCapacity 测试根据容量获取缓冲区
func TestBKPool_GetWithCapacity(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	// 测试不同大小的缓冲区
	tests := []struct {
		name     string
		capacity int
		minCap   int
	}{
		{"small", 50, 64},
		{"medium", 200, 304},
		{"large", 500, 544},
		{"xlarge", 1000, 1024},
		{"exact_min", 64, 64},
		{"exact_max", 1024, 1024},
		{"over_max", 2000, 1024},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			buf := pool.GetWithCapacity(tt.capacity)
			if buf == nil {
				t.Fatal("GetWithCapacity returned nil")
			}
			if cap(buf.B) < tt.minCap {
				t.Errorf("capacity: got %d, want >= %d", cap(buf.B), tt.minCap)
			}
			if len(buf.B) != 0 {
				t.Errorf("length: got %d, want 0", len(buf.B))
			}
			if buf.released.Load() {
				t.Error("released should be false after GetWithCapacity")
			}
			pool.Put(buf)
		})
	}
}

// TestBKPool_Put 测试归还缓冲区
func TestBKPool_Put(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	buf := pool.GetWithCapacity(100)
	if buf == nil {
		t.Fatal("GetWithCapacity returned nil")
	}

	// 设置一些数据
	buf.SetString("test data")
	originalCap := cap(buf.B)

	// 归还缓冲区
	pool.Put(buf)

	// 验证 released 标志
	if !buf.released.Load() {
		t.Error("released should be true after Put")
	}

	// 再次获取，应该能复用
	buf2 := pool.GetWithCapacity(100)
	if buf2 == nil {
		t.Fatal("GetWithCapacity after Put returned nil")
	}
	if cap(buf2.B) != originalCap {
		t.Logf("capacity changed: got %d, want %d (may be different due to pool behavior)", cap(buf2.B), originalCap)
	}
	if len(buf2.B) != 0 {
		t.Errorf("length should be 0 after reuse: got %d", len(buf2.B))
	}
	pool.Put(buf2)
}

// TestBKPool_Put_Duplicate 测试防止重复释放
func TestBKPool_Put_Duplicate(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	buf := pool.GetWithCapacity(100)
	if buf == nil {
		t.Fatal("GetWithCapacity returned nil")
	}

	// 第一次 Put 应该成功
	pool.Put(buf)
	if !buf.released.Load() {
		t.Error("released should be true after first Put")
	}

	// 第二次 Put 应该被忽略
	pool.Put(buf)
	if !buf.released.Load() {
		t.Error("released should still be true after second Put")
	}

	// 第三次 Put 也应该被忽略
	pool.Put(buf)
	if !buf.released.Load() {
		t.Error("released should still be true after third Put")
	}
}

// TestBKPool_index 测试索引计算
func TestBKPool_index(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	tests := []struct {
		size int
		want int
	}{
		{0, 0},
		{1, 0},
		{64, 0},
		{65, 1},
		{304, 1},
		{305, 2},
		{544, 2},
		{545, 3},
		{1024, 3},
		{1025, 3}, // 超过最大值，返回最后一个索引
		{2000, 3},
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			got := pool.index(tt.size)
			if got != tt.want {
				t.Errorf("index(%d) = %d, want %d", tt.size, got, tt.want)
			}
		})
	}
}

// TestBKPool_Concurrent 测试并发安全性
func TestBKPool_Concurrent(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	const goroutines = 100
	const iterations = 100

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				// 获取不同大小的缓冲区
				capacity := 64 + (j % 960) // 64 到 1024
				buf := pool.GetWithCapacity(capacity)
				if buf == nil {
					t.Errorf("goroutine %d, iteration %d: GetWithCapacity returned nil", id, j)
					return
				}
				if buf.released.Load() {
					t.Errorf("goroutine %d, iteration %d: released should be false", id, j)
					return
				}

				// 使用缓冲区
				buf.SetString("test data")
				if len(buf.B) == 0 {
					t.Errorf("goroutine %d, iteration %d: SetString failed", id, j)
					return
				}

				// 归还缓冲区
				pool.Put(buf)
				if !buf.released.Load() {
					t.Errorf("goroutine %d, iteration %d: released should be true after Put", id, j)
					return
				}
			}
		}(i)
	}

	wg.Wait()
}

// TestBKPool_DifferentSizes 测试不同大小的缓冲区池
func TestBKPool_DifferentSizes(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	// 获取不同大小的缓冲区
	sizes := []int{50, 100, 200, 500, 1000}
	buffers := make([]*ByteBuffer, len(sizes))

	for i, size := range sizes {
		buf := pool.GetWithCapacity(size)
		if buf == nil {
			t.Fatalf("GetWithCapacity(%d) returned nil", size)
		}
		buffers[i] = buf
	}

	// 验证每个缓冲区都有足够的容量
	for i, size := range sizes {
		if cap(buffers[i].B) < size {
			t.Errorf("buffer %d: cap %d < size %d", i, cap(buffers[i].B), size)
		}
	}

	// 归还所有缓冲区
	for _, buf := range buffers {
		pool.Put(buf)
	}
}

// TestBKPool_Reuse 测试缓冲区复用
func TestBKPool_Reuse(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	// 获取并归还一个缓冲区
	buf1 := pool.GetWithCapacity(100)
	buf1.SetString("test")
	pool.Put(buf1)

	// 再次获取相同大小的缓冲区，应该能复用
	buf2 := pool.GetWithCapacity(100)
	if buf2 == nil {
		t.Fatal("GetWithCapacity returned nil")
	}
	if len(buf2.B) != 0 {
		t.Errorf("reused buffer should be empty: len = %d", len(buf2.B))
	}
	// 注意：由于 sync.Pool 的特性，不保证一定是同一个对象
	// 但容量应该至少满足要求
	if cap(buf2.B) < 100 {
		t.Errorf("capacity: got %d, want >= 100", cap(buf2.B))
	}
	pool.Put(buf2)
}

// TestBKPool_EdgeCases 测试边界情况
func TestBKPool_EdgeCases(t *testing.T) {
	// 测试最小步数
	pool1 := NewBKPool(64, 128, 1)
	if pool1 == nil {
		t.Fatal("NewBKPool(1 step) returned nil")
	}
	buf1 := pool1.GetWithCapacity(100)
	if buf1 == nil {
		t.Fatal("GetWithCapacity from 1-step pool returned nil")
	}
	pool1.Put(buf1)

	// 测试相同的最小和最大大小
	pool2 := NewBKPool(64, 64, 1)
	if pool2 == nil {
		t.Fatal("NewBKPool(same min/max) returned nil")
	}
	buf2 := pool2.GetWithCapacity(64)
	if buf2 == nil {
		t.Fatal("GetWithCapacity returned nil")
	}
	if cap(buf2.B) < 64 {
		t.Errorf("capacity: got %d, want >= 64", cap(buf2.B))
	}
	pool2.Put(buf2)

	// 测试非常小的容量请求
	pool3 := NewBKPool(64, 1024, 4)
	buf3 := pool3.GetWithCapacity(1)
	if buf3 == nil {
		t.Fatal("GetWithCapacity(1) returned nil")
	}
	if cap(buf3.B) < 64 {
		t.Errorf("capacity: got %d, want >= 64", cap(buf3.B))
	}
	pool3.Put(buf3)

	// 测试超过最大值的容量请求
	buf4 := pool3.GetWithCapacity(2000)
	if buf4 == nil {
		t.Fatal("GetWithCapacity(2000) returned nil")
	}
	if cap(buf4.B) < 1024 {
		t.Errorf("capacity: got %d, want >= 1024", cap(buf4.B))
	}
	pool3.Put(buf4)
}

// TestBKPool_SizeDistribution 测试大小分布
func TestBKPool_SizeDistribution(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	// 计算实际的 sizeList 值
	// stepSize = (1024 - 64) / 4 = 240
	// sizeList[0] = 64
	// sizeList[1] = 64 + 240 = 304
	// sizeList[2] = 64 + 240*2 = 544
	// sizeList[3] = 1024

	// index 函数使用 <= 比较，所以：
	// size <= 64 -> index 0
	// size <= 304 -> index 1
	// size <= 544 -> index 2
	// size <= 1024 -> index 3

	// 测试每个步长范围内的缓冲区
	testRanges := []struct {
		name     string
		capacity int
		expected int // 期望的索引
	}{
		{"step0_min", 1, 0},
		{"step0", 64, 0},
		{"step1_min", 65, 1},
		{"step1", 304, 1},
		{"step2_min", 305, 2},
		{"step2", 544, 2},
		{"step3_min", 545, 3},
		{"step3", 1024, 3},
		{"over_max", 2000, 3},
	}

	for _, tt := range testRanges {
		t.Run(tt.name, func(t *testing.T) {
			index := pool.index(tt.capacity)
			if index != tt.expected {
				t.Errorf("index(%d) = %d, want %d", tt.capacity, index, tt.expected)
			}

			buf := pool.GetWithCapacity(tt.capacity)
			if buf == nil {
				t.Fatal("GetWithCapacity returned nil")
			}
			// 验证容量至少满足要求
			if cap(buf.B) < pool.sizeList[tt.expected] {
				t.Errorf("capacity: got %d, want >= %d", cap(buf.B), pool.sizeList[tt.expected])
			}
			pool.Put(buf)
		})
	}
}

// TestBKPool_MultiplePools 测试多个独立的池
func TestBKPool_MultiplePools(t *testing.T) {
	pool1 := NewBKPool(64, 256, 2)
	pool2 := NewBKPool(128, 512, 2)

	buf1 := pool1.GetWithCapacity(100)
	buf2 := pool2.GetWithCapacity(200)

	if buf1 == nil || buf2 == nil {
		t.Fatal("GetWithCapacity returned nil")
	}

	// 两个池应该独立工作
	if cap(buf1.B) == cap(buf2.B) {
		t.Log("Note: capacities may be the same by chance")
	}

	pool1.Put(buf1)
	pool2.Put(buf2)
}

// TestBKPool_ZeroSize 测试零大小
func TestBKPool_ZeroSize(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	buf := pool.GetWithCapacity(0)
	if buf == nil {
		t.Fatal("GetWithCapacity(0) returned nil")
	}
	// 应该返回最小大小的缓冲区
	if cap(buf.B) < 64 {
		t.Errorf("capacity: got %d, want >= 64", cap(buf.B))
	}
	pool.Put(buf)
}

// TestBKPool_NegativeSize 测试负数大小（应该使用最小大小）
func TestBKPool_NegativeSize(t *testing.T) {
	pool := NewBKPool(64, 1024, 4)

	buf := pool.GetWithCapacity(-1)
	if buf == nil {
		t.Fatal("GetWithCapacity(-1) returned nil")
	}
	// 负数应该被处理为使用第一个池
	if cap(buf.B) < 64 {
		t.Errorf("capacity: got %d, want >= 64", cap(buf.B))
	}
	pool.Put(buf)
}
