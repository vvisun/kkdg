package kkbuffer

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestPool_GetPut(t *testing.T) {
	pool := &Pool{}

	// Get a buffer
	buf1 := pool.Get()
	if buf1 == nil {
		t.Fatal("Get returned nil")
	}
	if len(buf1.B) != 0 {
		t.Fatalf("New buffer length is %d, expected 0", len(buf1.B))
	}

	// Write some data
	buf1.WriteString("test")

	// Put it back
	pool.Put(buf1)
	if !buf1.released.Load() {
		t.Fatal("Buffer was not released after Put")
	}

	pool.Put(buf1) //重复释放
	if !buf1.released.Load() {
		t.Fatal("Buffer was not released after Put")
	}

	// Get another buffer - should reuse the same one
	buf2 := pool.Get()
	if buf2 == nil {
		t.Fatal("Get returned nil after Put")
	}
	if len(buf2.B) != 0 {
		t.Fatalf("Reused buffer length is %d, expected 0", len(buf2.B))
	}
}

func TestGetPut(t *testing.T) {
	// Test the default pool functions
	buf1 := Get()
	if buf1 == nil {
		t.Fatal("Get returned nil")
	}

	buf1.WriteString("test")
	Put(buf1)

	buf2 := Get()
	if buf2 == nil {
		t.Fatal("Get returned nil after Put")
	}
	if len(buf2.B) != 0 {
		t.Fatalf("Reused buffer length is %d, expected 0", len(buf2.B))
	}
}

func TestPool_Concurrent(t *testing.T) {
	pool := &Pool{}
	var wg sync.WaitGroup
	numGoroutines := 100
	numIterations := 100

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < numIterations; j++ {
				buf := pool.Get()
				buf.WriteString("test")
				pool.Put(buf)
			}
		}()
	}

	wg.Wait()
}

func TestPool_LargeBuffer(t *testing.T) {
	pool := &Pool{}

	// Set a maxSize to test the behavior
	atomic.StoreUint64(&pool.maxSize, 1024)

	// Create a large buffer
	buf := pool.Get()
	largeData := make([]byte, 2048) // Larger than maxSize
	buf.Write(largeData)
	originalCap := cap(buf.B)

	// Put it back - should not be reused if it exceeds maxSize
	pool.Put(buf)

	// Get a new buffer
	buf2 := pool.Get()
	// If the buffer was reused, it would have the same capacity
	// If it wasn't reused (because it exceeded maxSize), it would be a new buffer
	if cap(buf2.B) == originalCap && originalCap > 1024 {
		t.Fatal("Large buffer was reused when it shouldn't be")
	}
}

func TestPool_Calibration(t *testing.T) {
	pool := &Pool{}

	// Trigger calibration by making many calls
	for i := 0; i < calibrateCallsThreshold+1; i++ {
		buf := pool.Get()
		buf.WriteString("test")
		pool.Put(buf)
	}

	// Calibration should have occurred
	// Get a buffer and verify it has reasonable capacity
	buf := pool.Get()
	if buf == nil {
		t.Fatal("Get returned nil")
	}
}

func TestPool_MultipleSizes(t *testing.T) {
	pool := &Pool{}

	// Test with different buffer sizes
	sizes := []int{64, 128, 256, 512, 1024}

	for _, size := range sizes {
		buf := pool.Get()
		data := make([]byte, size)
		buf.Write(data)
		pool.Put(buf)
	}

	// Get a buffer and verify it works
	buf := pool.Get()
	if buf == nil {
		t.Fatal("Get returned nil")
	}
}

func TestPool_ResetOnPut(t *testing.T) {
	pool := &Pool{}

	buf := pool.Get()
	buf.WriteString("test data")
	pool.Put(buf)

	buf2 := pool.Get()
	if len(buf2.B) != 0 {
		t.Fatalf("Buffer was not reset, length is %d, expected 0", len(buf2.B))
	}
}

func TestPool_EmptyPool(t *testing.T) {
	pool := &Pool{}

	// Get from empty pool
	buf := pool.Get()
	if buf == nil {
		t.Fatal("Get returned nil from empty pool")
	}
	if buf.B == nil {
		t.Fatal("Buffer B is nil")
	}
	if len(buf.B) != 0 {
		t.Fatalf("New buffer length is %d, expected 0", len(buf.B))
	}
}

func TestPool_Reuse(t *testing.T) {
	pool := &Pool{}

	// Get and put multiple times
	bufs := make([]*ByteBuffer, 10)
	for i := 0; i < 10; i++ {
		bufs[i] = pool.Get()
		bufs[i].WriteString("test")
	}

	// Put them all back
	for i := 0; i < 10; i++ {
		pool.Put(bufs[i])
	}

	// Get them again - should reuse
	for i := 0; i < 10; i++ {
		buf := pool.Get()
		if len(buf.B) != 0 {
			t.Fatalf("Reused buffer length is %d, expected 0", len(buf.B))
		}
	}
}

func TestIndex(t *testing.T) {
	tests := []struct {
		size int
		want int
	}{
		{0, 0},
		{1, 0},
		{64, 0},
		{65, 1},
		{128, 1},
		{129, 2},
		{256, 2},
		{512, 3},
		{1024, 4},
		{1000000, 14},  // Calculated based on actual index function logic
		{10000000, 18}, // Calculated based on actual index function logic
	}

	for _, tt := range tests {
		got := index(tt.size)
		if got != tt.want {
			t.Errorf("index(%d) = %d, want %d", tt.size, got, tt.want)
		}
	}
}
