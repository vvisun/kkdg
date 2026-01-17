package xbuffer

import (
	"testing"
)

func TestNewBytesPool(t *testing.T) {
	grade := 5
	pool := NewBytesPool(grade)

	if pool == nil {
		t.Fatal("NewBytesPool() should not return nil")
	}

	if len(pool.pools) != grade+1 {
		t.Errorf("pools length = %d, want %d", len(pool.pools), grade+1)
	}
}

func TestNewBytesPoolWithCapacity(t *testing.T) {
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
			pool := NewBytesPoolWithCapacity(tt.cap)
			if pool == nil {
				t.Fatal("NewBytesPoolWithCapacity() should not return nil")
			}
		})
	}
}

func TestBytesPool_Get(t *testing.T) {
	pool := NewBytesPool(5)

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
			bytes := pool.Get(tt.cap)
			if bytes == nil {
				t.Fatalf("Get(%d) should not return nil", tt.cap)
			}

			if bytes.Len() != tt.cap {
				t.Errorf("Len() = %d, want %d", bytes.Len(), tt.cap)
			}
		})
	}
}

func TestBytesPool_Get_Put(t *testing.T) {
	pool := NewBytesPool(5)

	// 获取对象
	bytes1 := pool.Get(10)
	if bytes1 == nil {
		t.Fatal("Get() should not return nil")
	}

	// 放回对象
	pool.Put(bytes1)

	// 再次获取，应该能复用
	bytes2 := pool.Get(10)
	if bytes2 == nil {
		t.Fatal("Get() should not return nil after Put")
	}
}

func TestBytesPool_Get_LargeCapacity(t *testing.T) {
	pool := NewBytesPool(5)

	// 测试超出池容量的请求
	bytes := pool.Get(1000)
	if bytes == nil {
		t.Fatal("Get() should not return nil even for large capacity")
	}
}

func TestMallocBytes(t *testing.T) {
	bytes := MallocBytes(100)
	if bytes == nil {
		t.Fatal("MallocBytes() should not return nil")
	}

	if bytes.Len() != 100 {
		t.Errorf("Len() = %d, want 100", bytes.Len())
	}
}

func TestBytesPool_Concurrent(t *testing.T) {
	pool := NewBytesPool(5)
	const numGoroutines = 10
	const operationsPerGoroutine = 100

	done := make(chan bool, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			defer func() { done <- true }()
			for j := 0; j < operationsPerGoroutine; j++ {
				bytes := pool.Get(10)
				if bytes == nil {
					t.Error("Get() should not return nil")
					return
				}
				pool.Put(bytes)
			}
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-done
	}
}
