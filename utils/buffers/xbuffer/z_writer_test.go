package xbuffer

import (
	"encoding/binary"
	"math"
	"testing"
)

func TestNewWriter(t *testing.T) {
	buf := make([]byte, 100)
	writer := NewWriter(buf)

	if writer == nil {
		t.Fatal("NewWriter() should not return nil")
	}

	if writer.Len() != 0 {
		t.Errorf("Len() = %d, want 0", writer.Len())
	}

	if writer.Cap() != 100 {
		t.Errorf("Cap() = %d, want 100", writer.Cap())
	}
}

func TestNewWriterWithCapacity(t *testing.T) {
	// 不指定容量
	writer1 := NewWriterWithCapacity()
	if writer1 == nil {
		t.Fatal("NewWriterWithCapacity() should not return nil")
	}

	// 指定容量
	writer2 := NewWriterWithCapacity(100)
	if writer2 == nil {
		t.Fatal("NewWriterWithCapacity(100) should not return nil")
	}

	if writer2.Cap() != 100 {
		t.Errorf("Cap() = %d, want 100", writer2.Cap())
	}
}

func TestWriter_Write(t *testing.T) {
	writer := NewWriterWithCapacity(100)
	data := []byte("hello world")

	n, err := writer.Write(data)
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if n != len(data) {
		t.Errorf("Write() returned %d, want %d", n, len(data))
	}

	if writer.Len() != len(data) {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(data))
	}

	if !equal(writer.Bytes(), data) {
		t.Errorf("Bytes() = %v, want %v", writer.Bytes(), data)
	}
}

func TestWriter_WriteBools(t *testing.T) {
	writer := NewWriterWithCapacity(10)
	values := []bool{true, false, true, true, false}

	writer.WriteBools(values...)

	if writer.Len() != len(values) {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values))
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadBool()
		if err != nil {
			t.Fatalf("ReadBool() error = %v", err)
		}
		if got != want {
			t.Errorf("ReadBool()[%d] = %v, want %v", i, got, want)
		}
	}
}

func TestWriter_WriteInt8s(t *testing.T) {
	writer := NewWriterWithCapacity(10)
	values := []int8{1, -1, 127, -128, 0}

	writer.WriteInt8s(values...)

	if writer.Len() != len(values) {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values))
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadInt8()
		if err != nil {
			t.Fatalf("ReadInt8() error = %v", err)
		}
		if got != want {
			t.Errorf("ReadInt8()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestWriter_WriteUint8s(t *testing.T) {
	writer := NewWriterWithCapacity(10)
	values := []uint8{0, 1, 255, 128, 64}

	writer.WriteUint8s(values...)

	if writer.Len() != len(values) {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values))
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadUint8()
		if err != nil {
			t.Fatalf("ReadUint8() error = %v", err)
		}
		if got != want {
			t.Errorf("ReadUint8()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestWriter_WriteInt16s(t *testing.T) {
	writer := NewWriterWithCapacity(20)
	values := []int16{1, -1, 32767, -32768, 0}

	writer.WriteInt16s(binary.BigEndian, values...)

	if writer.Len() != len(values)*2 {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values)*2)
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadInt16(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadInt16() error = %v", err)
		}
		if got != want {
			t.Errorf("ReadInt16()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestWriter_WriteUint16s(t *testing.T) {
	writer := NewWriterWithCapacity(20)
	values := []uint16{0, 1, 65535, 32768, 16384}

	writer.WriteUint16s(binary.BigEndian, values...)

	if writer.Len() != len(values)*2 {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values)*2)
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadUint16(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadUint16() error = %v", err)
		}
		if got != want {
			t.Errorf("ReadUint16()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestWriter_WriteInt32s(t *testing.T) {
	writer := NewWriterWithCapacity(40)
	values := []int32{1, -1, 2147483647, -2147483648, 0}

	writer.WriteInt32s(binary.BigEndian, values...)

	if writer.Len() != len(values)*4 {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values)*4)
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadInt32(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadInt32() error = %v", err)
		}
		if got != want {
			t.Errorf("ReadInt32()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestWriter_WriteUint32s(t *testing.T) {
	writer := NewWriterWithCapacity(40)
	values := []uint32{0, 1, 4294967295, 2147483648, 1073741824}

	writer.WriteUint32s(binary.BigEndian, values...)

	if writer.Len() != len(values)*4 {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values)*4)
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadUint32(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadUint32() error = %v", err)
		}
		if got != want {
			t.Errorf("ReadUint32()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestWriter_WriteInt64s(t *testing.T) {
	writer := NewWriterWithCapacity(80)
	values := []int64{1, -1, 9223372036854775807, -9223372036854775808, 0}

	writer.WriteInt64s(binary.BigEndian, values...)

	if writer.Len() != len(values)*8 {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values)*8)
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadInt64(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadInt64() error = %v", err)
		}
		if got != want {
			t.Errorf("ReadInt64()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestWriter_WriteUint64s(t *testing.T) {
	writer := NewWriterWithCapacity(80)
	values := []uint64{0, 1, 18446744073709551615, 9223372036854775808, 4611686018427387904}

	writer.WriteUint64s(binary.BigEndian, values...)

	if writer.Len() != len(values)*8 {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values)*8)
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadUint64(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadUint64() error = %v", err)
		}
		if got != want {
			t.Errorf("ReadUint64()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestWriter_WriteFloat32s(t *testing.T) {
	writer := NewWriterWithCapacity(40)
	values := []float32{0.0, 1.0, -1.0, 3.14, -3.14}

	writer.WriteFloat32s(binary.BigEndian, values...)

	if writer.Len() != len(values)*4 {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values)*4)
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadFloat32(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadFloat32() error = %v", err)
		}
		if math.Abs(float64(got-want)) > 0.0001 {
			t.Errorf("ReadFloat32()[%d] = %f, want %f", i, got, want)
		}
	}
}

func TestWriter_WriteFloat64s(t *testing.T) {
	writer := NewWriterWithCapacity(80)
	values := []float64{0.0, 1.0, -1.0, 3.141592653589793, -3.141592653589793}

	writer.WriteFloat64s(binary.BigEndian, values...)

	if writer.Len() != len(values)*8 {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values)*8)
	}

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadFloat64(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadFloat64() error = %v", err)
		}
		if math.Abs(got-want) > 0.0000001 {
			t.Errorf("ReadFloat64()[%d] = %f, want %f", i, got, want)
		}
	}
}

func TestWriter_WriteString(t *testing.T) {
	writer := NewWriterWithCapacity(100)
	str := "hello world"

	writer.WriteString(str)

	if writer.Len() != len(str) {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(str))
	}

	if string(writer.Bytes()) != str {
		t.Errorf("Bytes() = %s, want %s", string(writer.Bytes()), str)
	}
}

func TestWriter_WriteBytes(t *testing.T) {
	writer := NewWriterWithCapacity(100)
	values := []byte{1, 2, 3, 4, 5}

	writer.WriteBytes(values...)

	if writer.Len() != len(values) {
		t.Errorf("Len() = %d, want %d", writer.Len(), len(values))
	}

	if !equal(writer.Bytes(), values) {
		t.Errorf("Bytes() = %v, want %v", writer.Bytes(), values)
	}
}

func TestWriter_Grow(t *testing.T) {
	writer := NewWriterWithCapacity(10)

	// 写入数据，触发扩容
	data := make([]byte, 100)
	writer.Write(data)

	if writer.Cap() < 100 {
		t.Errorf("Cap() = %d, want >= 100", writer.Cap())
	}

	if writer.Len() != 100 {
		t.Errorf("Len() = %d, want 100", writer.Len())
	}
}

func TestWriter_Release(t *testing.T) {
	pool := NewWriterPool(5)
	writer := pool.Get(10)

	if writer == nil {
		t.Fatal("Get() should not return nil")
	}

	writer.WriteString("test")
	writer.Release()

	// 验证已重置
	if writer.Len() != 0 {
		t.Errorf("Len() after Release() = %d, want 0", writer.Len())
	}
}
