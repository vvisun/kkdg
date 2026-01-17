package xbuffer

import (
	"testing"
)

// equal 比较两个字节切片是否相等
func equal(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestNewBytes(t *testing.T) {
	buf := []byte("hello")
	bytes := NewBytes(buf)

	if bytes == nil {
		t.Fatal("NewBytes() should not return nil")
	}

	if bytes.Len() != len(buf) {
		t.Errorf("Len() = %d, want %d", bytes.Len(), len(buf))
	}

	if !equal(bytes.Bytes(), buf) {
		t.Errorf("Bytes() = %v, want %v", bytes.Bytes(), buf)
	}
}

func TestNewBytesWithCapacity(t *testing.T) {
	cap := 100
	bytes := NewBytesWithCapacity(cap)

	if bytes == nil {
		t.Fatal("NewBytesWithCapacity() should not return nil")
	}

	if bytes.Cap() < cap {
		t.Errorf("Cap() = %d, want >= %d", bytes.Cap(), cap)
	}

	if bytes.Len() != cap {
		t.Errorf("Len() = %d, want %d", bytes.Len(), cap)
	}
}

func TestBytes_Len(t *testing.T) {
	tests := []struct {
		name string
		buf  []byte
		want int
	}{
		{"empty", []byte{}, 0},
		{"small", []byte("hello"), 5},
		{"large", make([]byte, 1000), 1000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bytes := NewBytes(tt.buf)
			if bytes.Len() != tt.want {
				t.Errorf("Len() = %d, want %d", bytes.Len(), tt.want)
			}
		})
	}

	// 测试 nil
	var nilBytes *Bytes
	if nilBytes.Len() != 0 {
		t.Errorf("nil Bytes.Len() = %d, want 0", nilBytes.Len())
	}
}

func TestBytes_Cap(t *testing.T) {
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
			bytes := NewBytesWithCapacity(tt.cap)
			if bytes.Cap() < tt.cap {
				t.Errorf("Cap() = %d, want >= %d", bytes.Cap(), tt.cap)
			}
		})
	}

	// 测试 nil
	var nilBytes *Bytes
	if nilBytes.Cap() != 0 {
		t.Errorf("nil Bytes.Cap() = %d, want 0", nilBytes.Cap())
	}
}

func TestBytes_Available(t *testing.T) {
	bytes := NewBytesWithCapacity(100)
	bytes.off = 50

	if bytes.Available() != 50 {
		t.Errorf("Available() = %d, want 50", bytes.Available())
	}

	// 测试 nil
	var nilBytes *Bytes
	if nilBytes.Available() != 0 {
		t.Errorf("nil Bytes.Available() = %d, want 0", nilBytes.Available())
	}
}

func TestBytes_Bytes(t *testing.T) {
	buf := []byte("hello world")
	bytes := NewBytes(buf)

	result := bytes.Bytes()
	if !equal(result, buf) {
		t.Errorf("Bytes() = %v, want %v", result, buf)
	}

	// 测试 nil
	var nilBytes *Bytes
	if nilBytes.Bytes() != nil {
		t.Errorf("nil Bytes.Bytes() = %v, want nil", nilBytes.Bytes())
	}
}

func TestBytes_Release(t *testing.T) {
	pool := NewBytesPool(5)
	bytes := pool.Get(10)

	if bytes == nil {
		t.Fatal("Get() should not return nil")
	}

	bytes.Release()

	// 再次获取，应该能复用
	bytes2 := pool.Get(10)
	if bytes2 == nil {
		t.Fatal("Get() should not return nil after Release")
	}
}

func TestBytes_Release_DoubleRelease(t *testing.T) {
	pool := NewBytesPool(5)
	bytes := pool.Get(10)

	if bytes == nil {
		t.Fatal("Get() should not return nil")
	}

	// 第一次释放
	bytes.Release()

	// 第二次释放应该安全（不会panic或重复放回池中）
	bytes.Release()

	// 验证对象仍然可用（虽然已释放，但结构体本身仍然有效）
	if bytes.Cap() == 0 {
		t.Error("Cap() should not be 0 after double release")
	}
}
