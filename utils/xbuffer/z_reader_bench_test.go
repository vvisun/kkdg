package xbuffer

import (
	"encoding/binary"
	"testing"
)

var sinkBool bool
var sinkInt8 int8
var sinkUint8 uint8
var sinkInt16 int16
var sinkUint16 uint16
var sinkInt32 int32
var sinkUint32 uint32
var sinkInt64 int64
var sinkUint64 uint64
var sinkFloat32 float32
var sinkFloat64 float64
var sinkString string
var sinkByteSlice []byte

// BenchmarkReader_ReadBool 读取bool性能测试
func BenchmarkReader_ReadBool(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteBools(true, false, true, false, true)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkBool, _ = reader.ReadBool()
	}
}

// BenchmarkReader_ReadInt8 读取int8性能测试
func BenchmarkReader_ReadInt8(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteInt8s(1, 2, 3, 4, 5)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkInt8, _ = reader.ReadInt8()
	}
}

// BenchmarkReader_ReadUint8 读取uint8性能测试
func BenchmarkReader_ReadUint8(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteUint8s(1, 2, 3, 4, 5)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkUint8, _ = reader.ReadUint8()
	}
}

// BenchmarkReader_ReadInt16 读取int16性能测试
func BenchmarkReader_ReadInt16(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteInt16s(binary.BigEndian, 1, 2, 3, 4, 5)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkInt16, _ = reader.ReadInt16(binary.BigEndian)
	}
}

// BenchmarkReader_ReadUint16 读取uint16性能测试
func BenchmarkReader_ReadUint16(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteUint16s(binary.BigEndian, 1, 2, 3, 4, 5)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkUint16, _ = reader.ReadUint16(binary.BigEndian)
	}
}

// BenchmarkReader_ReadInt32 读取int32性能测试
func BenchmarkReader_ReadInt32(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteInt32s(binary.BigEndian, 1, 2, 3, 4, 5)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkInt32, _ = reader.ReadInt32(binary.BigEndian)
	}
}

// BenchmarkReader_ReadUint32 读取uint32性能测试
func BenchmarkReader_ReadUint32(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteUint32s(binary.BigEndian, 1, 2, 3, 4, 5)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkUint32, _ = reader.ReadUint32(binary.BigEndian)
	}
}

// BenchmarkReader_ReadInt64 读取int64性能测试
func BenchmarkReader_ReadInt64(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteInt64s(binary.BigEndian, 1, 2, 3, 4, 5)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkInt64, _ = reader.ReadInt64(binary.BigEndian)
	}
}

// BenchmarkReader_ReadUint64 读取uint64性能测试
func BenchmarkReader_ReadUint64(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteUint64s(binary.BigEndian, 1, 2, 3, 4, 5)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkUint64, _ = reader.ReadUint64(binary.BigEndian)
	}
}

// BenchmarkReader_ReadFloat32 读取float32性能测试
func BenchmarkReader_ReadFloat32(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteFloat32s(binary.BigEndian, 1.0, 2.0, 3.0, 4.0, 5.0)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkFloat32, _ = reader.ReadFloat32(binary.BigEndian)
	}
}

// BenchmarkReader_ReadFloat64 读取float64性能测试
func BenchmarkReader_ReadFloat64(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	writer.WriteFloat64s(binary.BigEndian, 1.0, 2.0, 3.0, 4.0, 5.0)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkFloat64, _ = reader.ReadFloat64(binary.BigEndian)
	}
}

// BenchmarkReader_ReadString 读取字符串性能测试
func BenchmarkReader_ReadString(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	str := "hello world"
	writer.WriteString(str)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkString, _ = reader.ReadString(len(str))
	}
}

// BenchmarkReader_ReadBytes 读取字节性能测试
func BenchmarkReader_ReadBytes(b *testing.B) {
	writer := NewWriterWithCapacity(100)
	data := []byte("hello world")
	writer.WriteBytes(data...)
	reader := NewReader(writer.Bytes())

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		reader.Reset()
		sinkByteSlice, _ = reader.ReadBytes(len(data))
	}
}

// BenchmarkReader_Seek 定位性能测试
func BenchmarkReader_Seek(b *testing.B) {
	data := make([]byte, 1000)
	reader := NewReader(data)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = reader.Seek(100, 0)
		reader.Reset()
	}
}
