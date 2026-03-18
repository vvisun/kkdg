package kkpacket

import (
	"testing"
)

func BenchmarkPacketHead_Marshal_Unmarshal(b *testing.B) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{}, &PartUint64{})
	data := make([]byte, head.GetSize())
	head.Marshal(data, 1, 2, 3)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := head.Unmarshal(data)
		if err != nil {
			b.Fatalf("unmarshal head: %v", err)
		}
	}
}

func BenchmarkPacketHead_UnmarshalTo(b *testing.B) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{}, &PartUint64{})
	data := make([]byte, head.GetSize())
	head.Marshal(data, 1, 2, 3)
	valueList := make([]int, head.GetPartCount())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := head.UnmarshalTo(data, valueList)
		if err != nil {
			b.Fatalf("unmarshal head: %v", err)
		}
	}
}
