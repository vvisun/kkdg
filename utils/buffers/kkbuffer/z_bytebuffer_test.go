package kkbuffer

import (
	"bytes"
	"io"
	"strings"
	"testing"
)

func TestByteBuffer_Write(t *testing.T) {
	b := &ByteBuffer{}
	n, err := b.Write([]byte("hello"))
	if err != nil {
		t.Fatalf("Write returned error: %v", err)
	}
	if n != 5 {
		t.Fatalf("Write returned %d, expected 5", n)
	}
	if string(b.B) != "hello" {
		t.Fatalf("Buffer contains %q, expected %q", string(b.B), "hello")
	}
}

func TestByteBuffer_WriteString(t *testing.T) {
	b := &ByteBuffer{}
	n, err := b.WriteString("world")
	if err != nil {
		t.Fatalf("WriteString returned error: %v", err)
	}
	if n != 5 {
		t.Fatalf("WriteString returned %d, expected 5", n)
	}
	if string(b.B) != "world" {
		t.Fatalf("Buffer contains %q, expected %q", string(b.B), "world")
	}
}

func TestByteBuffer_WriteByte(t *testing.T) {
	b := &ByteBuffer{}
	err := b.WriteByte('A')
	if err != nil {
		t.Fatalf("WriteByte returned error: %v", err)
	}
	if len(b.B) != 1 || b.B[0] != 'A' {
		t.Fatalf("Buffer contains %v, expected [65]", b.B)
	}
}

func TestByteBuffer_Reset(t *testing.T) {
	b := &ByteBuffer{}
	b.WriteString("test")
	b.Reset()
	if len(b.B) != 0 {
		t.Fatalf("Reset failed, buffer length is %d, expected 0", len(b.B))
	}
}

func TestByteBuffer_Set(t *testing.T) {
	b := &ByteBuffer{}
	b.WriteString("old")
	b.Set([]byte("new"))
	if string(b.B) != "new" {
		t.Fatalf("Set failed, buffer contains %q, expected %q", string(b.B), "new")
	}
}

func TestByteBuffer_SetString(t *testing.T) {
	b := &ByteBuffer{}
	b.WriteString("old")
	b.SetString("new")
	if string(b.B) != "new" {
		t.Fatalf("SetString failed, buffer contains %q, expected %q", string(b.B), "new")
	}
}

func TestByteBuffer_Len(t *testing.T) {
	b := &ByteBuffer{}
	if b.Len() != 0 {
		t.Fatalf("Empty buffer Len() returned %d, expected 0", b.Len())
	}
	b.WriteString("test")
	if b.Len() != 4 {
		t.Fatalf("Buffer Len() returned %d, expected 4", b.Len())
	}
}

func TestByteBuffer_Bytes(t *testing.T) {
	b := &ByteBuffer{}
	b.WriteString("test")
	bytes := b.Bytes()
	if string(bytes) != "test" {
		t.Fatalf("Bytes() returned %q, expected %q", string(bytes), "test")
	}
}

func TestByteBuffer_String(t *testing.T) {
	b := &ByteBuffer{}
	b.WriteString("test")
	if b.String() != "test" {
		t.Fatalf("String() returned %q, expected %q", b.String(), "test")
	}
}

func TestByteBuffer_ReadFrom(t *testing.T) {
	b := &ByteBuffer{}
	reader := strings.NewReader("hello world")
	n, err := b.ReadFrom(reader)
	if err != nil {
		t.Fatalf("ReadFrom returned error: %v", err)
	}
	if n != 11 {
		t.Fatalf("ReadFrom returned %d bytes, expected 11", n)
	}
	if string(b.B) != "hello world" {
		t.Fatalf("ReadFrom failed, buffer contains %q, expected %q", string(b.B), "hello world")
	}
}

func TestByteBuffer_ReadFrom_EmptyReader(t *testing.T) {
	b := &ByteBuffer{}
	reader := strings.NewReader("")
	n, err := b.ReadFrom(reader)
	if err != nil {
		t.Fatalf("ReadFrom returned error: %v", err)
	}
	if n != 0 {
		t.Fatalf("ReadFrom returned %d bytes, expected 0", n)
	}
	if len(b.B) != 0 {
		t.Fatalf("ReadFrom failed, buffer length is %d, expected 0", len(b.B))
	}
}

func TestByteBuffer_ReadFrom_LargeData(t *testing.T) {
	b := &ByteBuffer{}
	data := strings.Repeat("a", 10000)
	reader := strings.NewReader(data)
	n, err := b.ReadFrom(reader)
	if err != nil {
		t.Fatalf("ReadFrom returned error: %v", err)
	}
	if n != int64(len(data)) {
		t.Fatalf("ReadFrom returned %d bytes, expected %d", n, len(data))
	}
	if string(b.B) != data {
		t.Fatalf("ReadFrom failed for large data")
	}
}

func TestByteBuffer_WriteTo(t *testing.T) {
	b := &ByteBuffer{}
	b.WriteString("test data")
	var buf bytes.Buffer
	n, err := b.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo returned error: %v", err)
	}
	if n != 9 {
		t.Fatalf("WriteTo returned %d bytes, expected 9", n)
	}
	if buf.String() != "test data" {
		t.Fatalf("WriteTo failed, written data is %q, expected %q", buf.String(), "test data")
	}
}

func TestByteBuffer_WriteTo_EmptyBuffer(t *testing.T) {
	b := &ByteBuffer{}
	var buf bytes.Buffer
	n, err := b.WriteTo(&buf)
	if err != nil {
		t.Fatalf("WriteTo returned error: %v", err)
	}
	if n != 0 {
		t.Fatalf("WriteTo returned %d bytes, expected 0", n)
	}
	if buf.Len() != 0 {
		t.Fatalf("WriteTo failed, written data length is %d, expected 0", buf.Len())
	}
}

func TestByteBuffer_MultipleWrites(t *testing.T) {
	b := &ByteBuffer{}
	b.WriteString("hello")
	b.WriteString(" ")
	b.WriteString("world")
	if string(b.B) != "hello world" {
		t.Fatalf("Multiple writes failed, buffer contains %q, expected %q", string(b.B), "hello world")
	}
}

func TestByteBuffer_AppendBehavior(t *testing.T) {
	b := &ByteBuffer{}
	b.Write([]byte("hello"))
	b.Write([]byte("world"))
	if string(b.B) != "helloworld" {
		t.Fatalf("Append behavior failed, buffer contains %q, expected %q", string(b.B), "helloworld")
	}
}

func TestByteBuffer_ResetPreservesCapacity(t *testing.T) {
	b := &ByteBuffer{}
	b.WriteString(strings.Repeat("a", 100))
	capBefore := cap(b.B)
	b.Reset()
	capAfter := cap(b.B)
	if capAfter != capBefore {
		t.Fatalf("Reset changed capacity from %d to %d", capBefore, capAfter)
	}
	if len(b.B) != 0 {
		t.Fatalf("Reset failed, buffer length is %d, expected 0", len(b.B))
	}
}

func TestByteBuffer_ImplementsInterfaces(t *testing.T) {
	var _ io.Writer = &ByteBuffer{}
	var _ io.WriterTo = &ByteBuffer{}
	var _ io.ReaderFrom = &ByteBuffer{}
}
