package kkbuffer

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestPool_GetPut(t *testing.T) {
	pool := &bfPool{}

	buf := pool.Get()
	if buf == nil {
		t.Fatal("Get returned nil")
	}
	if len(buf.B) != 0 {
		t.Fatalf("new buffer len: got %d, want 0", len(buf.B))
	}

	buf.WriteString("test")
	pool.Put(buf)
	if !buf.released.Load() {
		t.Fatal("Put: released should be true")
	}

	// 重复 Put 应被忽略
	pool.Put(buf)

	buf2 := pool.Get()
	if buf2 == nil {
		t.Fatal("Get after Put returned nil")
	}
	if len(buf2.B) != 0 {
		t.Fatalf("reused buffer len: got %d, want 0", len(buf2.B))
	}
}

func TestGetPut(t *testing.T) {
	buf := Get()
	if buf == nil {
		t.Fatal("Get returned nil")
	}
	buf.WriteString("test")
	Put(buf)

	buf2 := Get()
	if buf2 == nil {
		t.Fatal("Get after Put returned nil")
	}
	if len(buf2.B) != 0 {
		t.Fatalf("reused buffer len: got %d, want 0", len(buf2.B))
	}
}

func TestPool_Concurrent(t *testing.T) {
	pool := &bfPool{}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				buf := pool.Get()
				buf.WriteString("test")
				pool.Put(buf)
			}
		}()
	}
	wg.Wait()
}

func TestPool_LargeBuffer(t *testing.T) {
	pool := &bfPool{}
	atomic.StoreUint64(&pool.maxSize, 1024)

	buf := pool.Get()
	large := make([]byte, 2048)
	buf.Write(large)
	origCap := cap(buf.B)
	pool.Put(buf)

	buf2 := pool.Get()
	// 超大 buffer 不应回池，buf2 应为新分配或小容量
	if cap(buf2.B) == origCap && origCap > 1024 {
		t.Fatal("oversized buffer should not be reused")
	}
}

func TestPool_GetWithCapacity(t *testing.T) {
	pool := &bfPool{}

	buf := pool.GetWithCapacity(100)
	if buf == nil {
		t.Fatal("GetWithCapacity returned nil")
	}
	if cap(buf.B) < 100 {
		t.Fatalf("cap: got %d, want >= 100", cap(buf.B))
	}
	if len(buf.B) != 0 {
		t.Fatalf("len: got %d, want 0", len(buf.B))
	}
	pool.Put(buf)

	// 指定较小容量时可能复用
	buf2 := pool.GetWithCapacity(50)
	if buf2 == nil {
		t.Fatal("GetWithCapacity(50) returned nil")
	}
	pool.Put(buf2)
}

func TestGetWithCapacity(t *testing.T) {
	buf := GetWithCapacity(256)
	if buf == nil {
		t.Fatal("GetWithCapacity returned nil")
	}
	if cap(buf.B) < 256 {
		t.Fatalf("cap: got %d, want >= 256", cap(buf.B))
	}
	Put(buf)
}

func TestPool_ResetOnPut(t *testing.T) {
	pool := &bfPool{}

	buf := pool.Get()
	buf.WriteString("data")
	pool.Put(buf)

	buf2 := pool.Get()
	if len(buf2.B) != 0 {
		t.Fatalf("Put should reset: len got %d, want 0", len(buf2.B))
	}
}

func TestPool_EmptyGet(t *testing.T) {
	pool := &bfPool{}

	buf := pool.Get()
	if buf == nil {
		t.Fatal("Get from empty pool returned nil")
	}
	if buf.B == nil {
		t.Fatal("buf.B is nil")
	}
	if len(buf.B) != 0 {
		t.Fatalf("new buffer len: got %d, want 0", len(buf.B))
	}
}

func TestIndex(t *testing.T) {
	tests := []struct {
		n    int
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
	}
	for _, tt := range tests {
		got := index(tt.n)
		if got != tt.want {
			t.Errorf("index(%d) = %d, want %d", tt.n, got, tt.want)
		}
	}
}

func TestPool_Calibrate(t *testing.T) {
	pool := &bfPool{}

	for i := 0; i < calibrateCallsThreshold+1; i++ {
		buf := pool.Get()
		buf.WriteString("x")
		pool.Put(buf)
	}

	buf := pool.Get()
	if buf == nil {
		t.Fatal("Get after calibrate returned nil")
	}
	Put(buf)
}
