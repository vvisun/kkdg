package buffer_test

import (
	"encoding/binary"
	"testing"

	buffer "github.com/vvisun/kkdg/utils/xbuffer"
)

func TestNocopyBuffer(t *testing.T) {
	writer := buffer.MallocWriter(9)
	writer.WriteInt32s(binary.BigEndian, 1029)
	writer.WriteInt8s(int8(0 << 7))
	writer.Release()

	writer.WriteInt32s(binary.BigEndian, 1029)
	writer.WriteInt8s(int8(0 << 7))

	// buf := buffer.NewNocopyBuffer()

	// buffer.MallocWriter()

	// data := []byte(xrand.Letters(1024))

	// for range 100 {
	// 	writer := buffer.MallocWriter(9)
	// 	writer.WriteInt32s(binary.BigEndian, 1029)
	// 	writer.WriteInt8s(int8(0 << 7))

	// 	buf := buffer.NewNocopyBuffer(writer, data)
	// 	buf.Release()
	// }
}
