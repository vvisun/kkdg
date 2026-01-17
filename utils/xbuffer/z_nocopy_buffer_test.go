package xbuffer

import (
	"testing"
)

func TestNewNocopyBuffer(t *testing.T) {
	buf := NewNocopyBuffer()
	if buf == nil {
		t.Fatal("NewNocopyBuffer() should not return nil")
	}

	if buf.Len() != 0 {
		t.Errorf("Len() = %d, want 0", buf.Len())
	}
}

func TestNocopyBuffer_Mount_ByteSlice(t *testing.T) {
	buf := NewNocopyBuffer()
	data := []byte("hello")

	buf.Mount(data)

	if buf.Len() != len(data) {
		t.Errorf("Len() = %d, want %d", buf.Len(), len(data))
	}

	if !equal(buf.Bytes(), data) {
		t.Errorf("Bytes() = %v, want %v", buf.Bytes(), data)
	}
}

func TestNocopyBuffer_Mount_Bytes(t *testing.T) {
	buf := NewNocopyBuffer()
	bytes := NewBytes([]byte("world"))

	buf.Mount(bytes)

	if buf.Len() != bytes.Len() {
		t.Errorf("Len() = %d, want %d", buf.Len(), bytes.Len())
	}

	if !equal(buf.Bytes(), bytes.Bytes()) {
		t.Errorf("Bytes() = %v, want %v", buf.Bytes(), bytes.Bytes())
	}
}

func TestNocopyBuffer_Mount_Writer(t *testing.T) {
	buf := NewNocopyBuffer()
	writer := NewWriterWithCapacity(100)
	writer.WriteString("test")

	buf.Mount(writer)

	if buf.Len() != writer.Len() {
		t.Errorf("Len() = %d, want %d", buf.Len(), writer.Len())
	}

	if !equal(buf.Bytes(), writer.Bytes()) {
		t.Errorf("Bytes() = %v, want %v", buf.Bytes(), writer.Bytes())
	}
}

func TestNocopyBuffer_Mount_Multiple(t *testing.T) {
	buf := NewNocopyBuffer()
	data1 := []byte("hello")
	data2 := []byte(" ")
	data3 := []byte("world")

	buf.Mount(data1)
	buf.Mount(data2)
	buf.Mount(data3)

	want := []byte("hello world")
	if buf.Len() != len(want) {
		t.Errorf("Len() = %d, want %d", buf.Len(), len(want))
	}

	if !equal(buf.Bytes(), want) {
		t.Errorf("Bytes() = %v, want %v", buf.Bytes(), want)
	}
}

func TestNocopyBuffer_Mount_Head(t *testing.T) {
	buf := NewNocopyBuffer()
	data1 := []byte("world")
	data2 := []byte("hello ")

	buf.Mount(data1)
	buf.Mount(data2, Head)

	want := []byte("hello world")
	if !equal(buf.Bytes(), want) {
		t.Errorf("Bytes() = %v, want %v", buf.Bytes(), want)
	}
}

func TestNocopyBuffer_MallocBytes(t *testing.T) {
	buf := NewNocopyBuffer()
	bytes := buf.MallocBytes(100)

	if bytes == nil {
		t.Fatal("MallocBytes() should not return nil")
	}

	if buf.Len() != 100 {
		t.Errorf("Len() = %d, want 100", buf.Len())
	}
}

func TestNocopyBuffer_MallocWriter(t *testing.T) {
	buf := NewNocopyBuffer()
	writer := buf.MallocWriter(100)

	if writer == nil {
		t.Fatal("MallocWriter() should not return nil")
	}

	writer.WriteString("test")
	if buf.Len() != 4 {
		t.Errorf("Len() = %d, want 4", buf.Len())
	}
}

func TestNocopyBuffer_Visit(t *testing.T) {
	buf := NewNocopyBuffer()
	data1 := []byte("hello")
	data2 := []byte("world")

	buf.Mount(data1)
	buf.Mount(data2)

	count := 0
	buf.Visit(func(node *NocopyNode) bool {
		count++
		return true
	})

	if count != 2 {
		t.Errorf("Visit() count = %d, want 2", count)
	}
}

func TestNocopyBuffer_Visit_Stop(t *testing.T) {
	buf := NewNocopyBuffer()
	data1 := []byte("hello")
	data2 := []byte("world")
	data3 := []byte("test")

	buf.Mount(data1)
	buf.Mount(data2)
	buf.Mount(data3)

	count := 0
	buf.Visit(func(node *NocopyNode) bool {
		count++
		return false // 停止遍历
	})

	if count != 1 {
		t.Errorf("Visit() count = %d, want 1", count)
	}
}

func TestNocopyBuffer_Release(t *testing.T) {
	buf := NewNocopyBuffer()
	data := []byte("hello")
	buf.Mount(data)

	buf.Release()

	if buf.Len() != 0 {
		t.Errorf("Len() after Release() = %d, want 0", buf.Len())
	}
}

func TestNocopyBuffer_Delay(t *testing.T) {
	buf := NewNocopyBuffer()
	data := []byte("hello")
	buf.Mount(data)

	// 设置延迟释放
	buf.Delay(3)

	// 前两次释放应该不生效
	buf.Release()
	if buf.Len() == 0 {
		t.Error("Release() should not work with delay > 0")
	}

	buf.Release()
	if buf.Len() == 0 {
		t.Error("Release() should not work with delay > 0")
	}

	// 第三次释放应该生效
	buf.Release()
	if buf.Len() != 0 {
		t.Errorf("Len() after final Release() = %d, want 0", buf.Len())
	}
}

func TestNocopyBuffer_Nested(t *testing.T) {
	buf1 := NewNocopyBuffer()
	buf1.Mount([]byte("hello"))

	buf2 := NewNocopyBuffer()
	buf2.Mount([]byte("world"))

	buf1.Mount(buf2)

	if buf1.Len() != 10 {
		t.Errorf("Len() = %d, want 10", buf1.Len())
	}

	want := []byte("helloworld")
	if !equal(buf1.Bytes(), want) {
		t.Errorf("Bytes() = %v, want %v", buf1.Bytes(), want)
	}
}
