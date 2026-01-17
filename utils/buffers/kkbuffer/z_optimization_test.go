package kkbuffer

import (
	"testing"
)

func TestSet_Optimized(t *testing.T) {
	buf := Get()
	defer Put(buf)

	// Test with sufficient capacity
	data1 := make([]byte, 100)
	for i := range data1 {
		data1[i] = byte(i)
	}
	buf.Write(data1) // Grow capacity
	buf.Reset()

	buf.Set(data1)
	if len(buf.B) != len(data1) {
		t.Fatalf("Set length mismatch: got %d, want %d", len(buf.B), len(data1))
	}
	for i := range data1 {
		if buf.B[i] != data1[i] {
			t.Fatalf("Set data mismatch at index %d", i)
		}
	}

	// Test with insufficient capacity
	data2 := make([]byte, 10000)
	for i := range data2 {
		data2[i] = byte(i % 256)
	}
	buf.Set(data2)
	if len(buf.B) != len(data2) {
		t.Fatalf("Set length mismatch: got %d, want %d", len(buf.B), len(data2))
	}
}

func TestSetString_Optimized(t *testing.T) {
	buf := Get()
	defer Put(buf)

	// Test with sufficient capacity
	data1 := "test string"
	buf.WriteString(data1)
	buf.Reset()

	buf.SetString(data1)
	if buf.String() != data1 {
		t.Fatalf("SetString mismatch: got %s, want %s", buf.String(), data1)
	}

	// Test with insufficient capacity
	data2 := string(make([]byte, 10000))
	buf.SetString(data2)
	if len(buf.B) != len(data2) {
		t.Fatalf("SetString length mismatch: got %d, want %d", len(buf.B), len(data2))
	}
}

func TestSetWithCapacity(t *testing.T) {
	buf := Get()
	defer Put(buf)

	data := make([]byte, 1000)
	for i := range data {
		data[i] = byte(i % 256)
	}

	buf.SetWithCapacity(data)
	if len(buf.B) != len(data) {
		t.Fatalf("SetWithCapacity length mismatch: got %d, want %d", len(buf.B), len(data))
	}
	if cap(buf.B) < len(data) {
		t.Fatalf("SetWithCapacity capacity too small: got %d, want >= %d", cap(buf.B), len(data))
	}
	for i := range data {
		if buf.B[i] != data[i] {
			t.Fatalf("SetWithCapacity data mismatch at index %d", i)
		}
	}
}

func TestGrow(t *testing.T) {
	buf := Get()
	defer Put(buf)

	// Test growing from zero
	buf.Grow(1000)
	if cap(buf.B) < 1000 {
		t.Fatalf("Grow failed: capacity %d < 1000", cap(buf.B))
	}
	if len(buf.B) != 0 {
		t.Fatalf("Grow should not change length: got %d, want 0", len(buf.B))
	}

	// Test growing with existing data
	buf.WriteString("test")
	oldLen := len(buf.B)
	buf.Grow(2000)
	if cap(buf.B) < 2000 {
		t.Fatalf("Grow failed: capacity %d < 2000", cap(buf.B))
	}
	if len(buf.B) != oldLen {
		t.Fatalf("Grow should preserve length: got %d, want %d", len(buf.B), oldLen)
	}
	if buf.String() != "test" {
		t.Fatalf("Grow should preserve data: got %s, want test", buf.String())
	}

	// Test growing when capacity is already sufficient
	oldCap := cap(buf.B)
	buf.Grow(1000)
	if cap(buf.B) != oldCap {
		t.Fatalf("Grow should not change capacity when sufficient: got %d, want %d", cap(buf.B), oldCap)
	}
}

func TestGetWithCapacity(t *testing.T) {
	// Test with small capacity
	buf1 := GetWithCapacity(100)
	if cap(buf1.B) < 100 {
		t.Fatalf("GetWithCapacity failed: capacity %d < 100", cap(buf1.B))
	}
	if len(buf1.B) != 0 {
		t.Fatalf("GetWithCapacity should return empty buffer: got %d, want 0", len(buf1.B))
	}
	Put(buf1)

	// Test with large capacity
	buf2 := GetWithCapacity(10000)
	if cap(buf2.B) < 10000 {
		t.Fatalf("GetWithCapacity failed: capacity %d < 10000", cap(buf2.B))
	}
	Put(buf2)

	// Test reuse with sufficient capacity
	buf3 := GetWithCapacity(100)
	oldCap := cap(buf3.B)
	buf3.WriteString("test")
	Put(buf3)

	buf4 := GetWithCapacity(50) // Less than old capacity
	if cap(buf4.B) != oldCap {
		t.Fatalf("GetWithCapacity should reuse buffer: capacity %d != %d", cap(buf4.B), oldCap)
	}
	if len(buf4.B) != 0 {
		t.Fatalf("Reused buffer should be empty: got %d, want 0", len(buf4.B))
	}
	Put(buf4)
}

func TestSet_Performance(t *testing.T) {
	buf := Get()
	defer Put(buf)

	// Pre-allocate capacity
	buf.Grow(1000)
	buf.Reset()

	data := make([]byte, 500)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Test that Set uses copy when capacity is sufficient
	buf.Set(data)
	if len(buf.B) != len(data) {
		t.Fatalf("Set length mismatch")
	}
	// Verify data is correct
	for i := range data {
		if buf.B[i] != data[i] {
			t.Fatalf("Set data mismatch at index %d", i)
		}
	}
}
