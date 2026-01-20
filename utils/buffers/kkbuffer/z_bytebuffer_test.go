package kkbuffer

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestByteBuffer_Len(t *testing.T) {
	buf := Get()
	defer Put(buf)

	if buf.Len() != 0 {
		t.Fatalf("new buffer Len: got %d, want 0", buf.Len())
	}
	buf.SetString("hello")
	if buf.Len() != 5 {
		t.Fatalf("after WriteString: got %d, want 5", buf.Len())
	}
	buf.Reset()
	if buf.Len() != 0 {
		t.Fatalf("after Reset: got %d, want 0", buf.Len())
	}
}

func TestByteBuffer_WriteString(t *testing.T) {
	buf := Get()
	defer Put(buf)

	s := "hello"
	buf.SetString(s)
	if buf.String() != s {
		t.Fatalf("SetString: got %q, want %q", buf.String(), s)
	}
}

func TestByteBuffer_Set(t *testing.T) {
	buf := Get()
	defer Put(buf)

	// 先写入一些数据以获得容量
	buf.SetString(strings.Repeat("x", 100))
	buf.Reset()

	data := []byte("hello")
	buf.SetBytes(data)
	if !bytes.Equal(buf.B, data) {
		t.Fatalf("Set: got %q, want %q", buf.B, data)
	}
	if len(buf.B) != len(data) {
		t.Fatalf("Set len: got %d, want %d", len(buf.B), len(data))
	}

	// 测试容量不足时
	large := make([]byte, 10000)
	for i := range large {
		large[i] = byte(i % 256)
	}
	buf.SetBytes(large)
	if len(buf.B) != len(large) {
		t.Fatalf("Set large len: got %d, want %d", len(buf.B), len(large))
	}
	if !bytes.Equal(buf.B, large) {
		t.Fatal("Set large: content mismatch")
	}
}

func TestByteBuffer_SetString(t *testing.T) {
	buf := Get()
	defer Put(buf)

	buf.SetString("xxxxx")
	buf.Reset()

	s := "world"
	buf.SetString(s)
	if buf.String() != s {
		t.Fatalf("SetString: got %q, want %q", buf.String(), s)
	}
}

func TestByteBuffer_Grow(t *testing.T) {
	buf := Get()
	defer Put(buf)

	// 从零增长
	buf.Grow(1000)
	if cap(buf.B) < 1000 {
		t.Fatalf("Grow(1000) cap: got %d, want >= 1000", cap(buf.B))
	}
	if len(buf.B) != 0 {
		t.Fatalf("Grow should not change len: got %d, want 0", len(buf.B))
	}

	// 有数据时增长
	buf.SetString("abc")
	oldLen := len(buf.B)
	buf.Grow(2000)
	if cap(buf.B) < 2000 {
		t.Fatalf("Grow(2000) cap: got %d, want >= 2000", cap(buf.B))
	}
	if len(buf.B) != oldLen {
		t.Fatalf("Grow should preserve len: got %d, want %d", len(buf.B), oldLen)
	}
	if buf.String() != "abc" {
		t.Fatalf("Grow should preserve data: got %q, want abc", buf.String())
	}

	// 容量已足够时不增长
	oldCap := cap(buf.B)
	buf.Grow(1000)
	if cap(buf.B) != oldCap {
		t.Fatalf("Grow when sufficient: cap changed from %d to %d", oldCap, cap(buf.B))
	}
}

func TestByteBuffer_Reset(t *testing.T) {
	buf := Get()
	defer Put(buf)

	buf.SetString("test")
	buf.Reset()
	if len(buf.B) != 0 {
		t.Fatalf("Reset: len got %d, want 0", len(buf.B))
	}
	if cap(buf.B) == 0 {
		t.Fatal("Reset: should preserve capacity")
	}
}

func TestByteBuffer_ReadFrom(t *testing.T) {
	buf := Get()
	defer Put(buf)

	r := strings.NewReader("hello world")
	n, err := buf.ReadFrom(r)
	if err != nil {
		t.Fatalf("ReadFrom: %v", err)
	}
	if n != 11 {
		t.Fatalf("ReadFrom n: got %d, want 11", n)
	}
	if buf.String() != "hello world" {
		t.Fatalf("ReadFrom: got %q, want hello world", buf.String())
	}
}

func TestByteBuffer_WriteTo(t *testing.T) {
	buf := Get()
	defer Put(buf)
	buf.SetString("foo")

	var w bytes.Buffer
	n, err := buf.WriteTo(&w)
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if n != 3 {
		t.Fatalf("WriteTo n: got %d, want 3", n)
	}
	if w.String() != "foo" {
		t.Fatalf("WriteTo: got %q, want foo", w.String())
	}
}

func TestByteBuffer_Bytes(t *testing.T) {
	buf := Get()
	defer Put(buf)
	buf.SetString("xyz")

	b := buf.Bytes()
	if !bytes.Equal(b, []byte("xyz")) {
		t.Fatalf("Bytes: got %q, want xyz", b)
	}
	// 修改 b 会影响 buf.B，这是预期行为
	b[0] = 'a'
	if buf.B[0] != 'a' {
		t.Fatal("Bytes: should return same slice")
	}
}

func TestByteBuffer_String(t *testing.T) {
	buf := Get()
	defer Put(buf)
	buf.SetString("test")

	if buf.String() != "test" {
		t.Fatalf("String: got %q, want test", buf.String())
	}
}

func TestByteBuffer_ReadFrom_EOF(t *testing.T) {
	buf := Get()
	defer Put(buf)

	r := strings.NewReader("hi")
	n, err := buf.ReadFrom(r)
	if err != nil {
		t.Fatalf("ReadFrom to EOF: %v", err)
	}
	if n != 2 {
		t.Fatalf("ReadFrom n: got %d, want 2", n)
	}
	if buf.String() != "hi" {
		t.Fatalf("ReadFrom: got %q, want hi", buf.String())
	}
}

func TestByteBuffer_ReadFrom_Error(t *testing.T) {
	buf := Get()
	defer Put(buf)

	r := &errorReader{err: io.ErrClosedPipe}
	_, err := buf.ReadFrom(r)
	if err != io.ErrClosedPipe {
		t.Fatalf("ReadFrom: got %v, want ErrClosedPipe", err)
	}
}

type errorReader struct{ err error }

func (e *errorReader) Read([]byte) (int, error) { return 0, e.err }
