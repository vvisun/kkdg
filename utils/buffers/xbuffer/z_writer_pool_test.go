package xbuffer

import (
	"testing"
)

func TestNewWriterPool(t *testing.T) {
	grade := 5
	pool := NewWriterPool(grade)

	if pool == nil {
		t.Fatal("NewWriterPool() should not return nil")
	}

	if len(pool.pools) != grade+1 {
		t.Errorf("pools length = %d, want %d", len(pool.pools), grade+1)
	}
}

func TestNewWriterPoolWithCapacity(t *testing.T) {
	tests := []struct {
		name string
		cap  int
	}{
		{"small", 10},
		{"medium", 100},
		{"large", 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pool := NewWriterPoolWithCapacity(tt.cap)
			if pool == nil {
				t.Fatal("NewWriterPoolWithCapacity() should not return nil")
			}
		})
	}
}

func TestWriterPool_Get(t *testing.T) {
	pool := NewWriterPool(5)

	tests := []struct {
		name string
		cap  int
	}{
		{"1", 1},
		{"2", 2},
		{"4", 4},
		{"8", 8},
		{"16", 16},
		{"32", 32},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			writer := pool.Get(tt.cap)
			if writer == nil {
				t.Fatalf("Get(%d) should not return nil", tt.cap)
			}

			if writer.Cap() < tt.cap {
				t.Errorf("Cap() = %d, want >= %d", writer.Cap(), tt.cap)
			}
		})
	}
}

func TestWriterPool_Get_Put(t *testing.T) {
	pool := NewWriterPool(5)

	// 获取对象
	writer1 := pool.Get(10)
	if writer1 == nil {
		t.Fatal("Get() should not return nil")
	}

	writer1.WriteString("test")

	// 放回对象前先释放
	writer1.Release()
	pool.Put(writer1)

	// 再次获取，应该能复用
	writer2 := pool.Get(10)
	if writer2 == nil {
		t.Fatal("Get() should not return nil after Put")
	}

	// 验证已重置
	if writer2.Len() != 0 {
		t.Errorf("Len() = %d, want 0", writer2.Len())
	}
}

func TestMallocWriter(t *testing.T) {
	writer := MallocWriter(100)
	if writer == nil {
		t.Fatal("MallocWriter() should not return nil")
	}

	if writer.Cap() < 100 {
		t.Errorf("Cap() = %d, want >= 100", writer.Cap())
	}
}

func TestWriterPool_Concurrent(t *testing.T) {
	pool := NewWriterPool(5)
	const numGoroutines = 10
	const operationsPerGoroutine = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < operationsPerGoroutine; j++ {
				writer := pool.Get(10)
				if writer == nil {
					t.Error("Get() should not return nil")
					return
				}
				writer.WriteString("test")
				pool.Put(writer)
			}
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
