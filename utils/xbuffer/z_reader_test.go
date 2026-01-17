package xbuffer

import (
	"encoding/binary"
	"io"
	"math"
	"testing"
)

func TestNewReader(t *testing.T) {
	data := []byte("hello world")
	reader := NewReader(data)

	if reader == nil {
		t.Fatal("NewReader() should not return nil")
	}
}

func TestReader_Seek(t *testing.T) {
	data := []byte("hello world")
	reader := NewReader(data)

	tests := []struct {
		name   string
		offset int64
		whence int
		want   int64
	}{
		{"SeekStart", 5, io.SeekStart, 5},
		{"SeekCurrent", 3, io.SeekCurrent, 3},
		{"SeekEnd", -5, io.SeekEnd, int64(len(data)) - 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader.Reset()
			pos, err := reader.Seek(tt.offset, tt.whence)
			if err != nil {
				t.Fatalf("Seek() error = %v", err)
			}
			if pos != tt.want {
				t.Errorf("Seek() = %d, want %d", pos, tt.want)
			}
		})
	}
}

func TestReader_Seek_InvalidWhence(t *testing.T) {
	reader := NewReader([]byte("test"))
	_, err := reader.Seek(0, 999)
	if err == nil {
		t.Error("Seek() should return error for invalid whence")
	}
}

func TestReader_Seek_NegativePosition(t *testing.T) {
	reader := NewReader([]byte("test"))
	_, err := reader.Seek(-1, io.SeekStart)
	if err == nil {
		t.Error("Seek() should return error for negative position")
	}
}

func TestReader_ReadBool(t *testing.T) {
	writer := NewWriterWithCapacity(10)
	writer.WriteBools(true, false, true)

	reader := NewReader(writer.Bytes())

	got, err := reader.ReadBool()
	if err != nil {
		t.Fatalf("ReadBool() error = %v", err)
	}
	if !got {
		t.Error("ReadBool() = false, want true")
	}

	got, err = reader.ReadBool()
	if err != nil {
		t.Fatalf("ReadBool() error = %v", err)
	}
	if got {
		t.Error("ReadBool() = true, want false")
	}
}

func TestReader_ReadBools(t *testing.T) {
	writer := NewWriterWithCapacity(10)
	values := []bool{true, false, true, true, false}
	writer.WriteBools(values...)

	reader := NewReader(writer.Bytes())
	got, err := reader.ReadBools(len(values))
	if err != nil {
		t.Fatalf("ReadBools() error = %v", err)
	}

	if len(got) != len(values) {
		t.Errorf("ReadBools() length = %d, want %d", len(got), len(values))
	}

	for i, want := range values {
		if got[i] != want {
			t.Errorf("ReadBools()[%d] = %v, want %v", i, got[i], want)
		}
	}
}

func TestReader_ReadInt8(t *testing.T) {
	writer := NewWriterWithCapacity(10)
	writer.WriteInt8s(1, -1, 127, -128)

	reader := NewReader(writer.Bytes())

	wants := []int8{1, -1, 127, -128}
	for i, want := range wants {
		got, err := reader.ReadInt8()
		if err != nil {
			t.Fatalf("ReadInt8()[%d] error = %v", i, err)
		}
		if got != want {
			t.Errorf("ReadInt8()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestReader_ReadUint8(t *testing.T) {
	writer := NewWriterWithCapacity(10)
	writer.WriteUint8s(0, 1, 255, 128)

	reader := NewReader(writer.Bytes())

	wants := []uint8{0, 1, 255, 128}
	for i, want := range wants {
		got, err := reader.ReadUint8()
		if err != nil {
			t.Fatalf("ReadUint8()[%d] error = %v", i, err)
		}
		if got != want {
			t.Errorf("ReadUint8()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestReader_ReadInt16(t *testing.T) {
	writer := NewWriterWithCapacity(20)
	values := []int16{1, -1, 32767, -32768}
	writer.WriteInt16s(binary.BigEndian, values...)

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadInt16(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadInt16()[%d] error = %v", i, err)
		}
		if got != want {
			t.Errorf("ReadInt16()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestReader_ReadUint16(t *testing.T) {
	writer := NewWriterWithCapacity(20)
	values := []uint16{0, 1, 65535, 32768}
	writer.WriteUint16s(binary.BigEndian, values...)

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadUint16(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadUint16()[%d] error = %v", i, err)
		}
		if got != want {
			t.Errorf("ReadUint16()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestReader_ReadInt32(t *testing.T) {
	writer := NewWriterWithCapacity(40)
	values := []int32{1, -1, 2147483647, -2147483648}
	writer.WriteInt32s(binary.BigEndian, values...)

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadInt32(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadInt32()[%d] error = %v", i, err)
		}
		if got != want {
			t.Errorf("ReadInt32()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestReader_ReadUint32(t *testing.T) {
	writer := NewWriterWithCapacity(40)
	values := []uint32{0, 1, 4294967295, 2147483648}
	writer.WriteUint32s(binary.BigEndian, values...)

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadUint32(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadUint32()[%d] error = %v", i, err)
		}
		if got != want {
			t.Errorf("ReadUint32()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestReader_ReadInt64(t *testing.T) {
	writer := NewWriterWithCapacity(80)
	values := []int64{1, -1, 9223372036854775807, -9223372036854775808}
	writer.WriteInt64s(binary.BigEndian, values...)

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadInt64(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadInt64()[%d] error = %v", i, err)
		}
		if got != want {
			t.Errorf("ReadInt64()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestReader_ReadUint64(t *testing.T) {
	writer := NewWriterWithCapacity(80)
	values := []uint64{0, 1, 18446744073709551615, 9223372036854775808}
	writer.WriteUint64s(binary.BigEndian, values...)

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadUint64(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadUint64()[%d] error = %v", i, err)
		}
		if got != want {
			t.Errorf("ReadUint64()[%d] = %d, want %d", i, got, want)
		}
	}
}

func TestReader_ReadFloat32(t *testing.T) {
	writer := NewWriterWithCapacity(40)
	values := []float32{0.0, 1.0, -1.0, 3.14, -3.14}
	writer.WriteFloat32s(binary.BigEndian, values...)

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadFloat32(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadFloat32()[%d] error = %v", i, err)
		}
		if math.Abs(float64(got-want)) > 0.0001 {
			t.Errorf("ReadFloat32()[%d] = %f, want %f", i, got, want)
		}
	}
}

func TestReader_ReadFloat64(t *testing.T) {
	writer := NewWriterWithCapacity(80)
	values := []float64{0.0, 1.0, -1.0, 3.141592653589793, -3.141592653589793}
	writer.WriteFloat64s(binary.BigEndian, values...)

	reader := NewReader(writer.Bytes())
	for i, want := range values {
		got, err := reader.ReadFloat64(binary.BigEndian)
		if err != nil {
			t.Fatalf("ReadFloat64()[%d] error = %v", i, err)
		}
		if math.Abs(got-want) > 0.0000001 {
			t.Errorf("ReadFloat64()[%d] = %f, want %f", i, got, want)
		}
	}
}

func TestReader_ReadString(t *testing.T) {
	writer := NewWriterWithCapacity(100)
	str := "hello world"
	writer.WriteString(str)

	reader := NewReader(writer.Bytes())
	got, err := reader.ReadString(len(str))
	if err != nil {
		t.Fatalf("ReadString() error = %v", err)
	}

	if got != str {
		t.Errorf("ReadString() = %s, want %s", got, str)
	}
}

func TestReader_ReadBytes(t *testing.T) {
	writer := NewWriterWithCapacity(100)
	values := []byte{1, 2, 3, 4, 5}
	writer.WriteBytes(values...)

	reader := NewReader(writer.Bytes())
	got, err := reader.ReadBytes(len(values))
	if err != nil {
		t.Fatalf("ReadBytes() error = %v", err)
	}

	if !equal(got, values) {
		t.Errorf("ReadBytes() = %v, want %v", got, values)
	}
}

func TestReader_Read_EOF(t *testing.T) {
	reader := NewReader([]byte("test"))

	// 读取超出范围
	_, err := reader.ReadBytes(100)
	if err == nil {
		t.Error("ReadBytes() should return error for EOF")
	}
}

func TestReader_Reset(t *testing.T) {
	reader := NewReader([]byte("hello world"))

	// 读取一些数据
	reader.ReadBytes(5)

	// 重置
	reader.Reset()

	// 应该能从开头读取
	data, err := reader.ReadBytes(5)
	if err != nil {
		t.Fatalf("ReadBytes() error = %v", err)
	}

	if string(data) != "hello" {
		t.Errorf("ReadBytes() after Reset() = %s, want hello", string(data))
	}
}
