package testpacket

import (
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
)

func TestKK_packet_Head_Marshal_Unmarshal(t *testing.T) {
	head := kkpacket.NewPacketHead(&kkpacket.PartUint16{}, &kkpacket.PartUint32{}, &kkpacket.PartUint64{})
	data := make([]byte, head.GetSize())
	head.Marshal(data, binary.BigEndian, 1, 2, 3)
	fmt.Printf("data: %v\n", data)

	valueList, err := head.Unmarshal(data, binary.BigEndian)
	if err != nil {
		t.Fatalf("unmarshal head: %v", err)
	}
	fmt.Printf("valueList: %v\n", valueList)
}

func BenchmarkPacketHead_Marshal_Unmarshal(b *testing.B) {
	head := kkpacket.NewPacketHead(&kkpacket.PartUint16{}, &kkpacket.PartUint32{}, &kkpacket.PartUint64{})
	data := make([]byte, head.GetSize())
	head.Marshal(data, binary.BigEndian, 1, 2, 3)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := head.Unmarshal(data, binary.BigEndian)
		if err != nil {
			b.Fatalf("unmarshal head: %v", err)
		}
	}
}

func BenchmarkPacketHead_UnmarshalTo(b *testing.B) {
	head := kkpacket.NewPacketHead(&kkpacket.PartUint16{}, &kkpacket.PartUint32{}, &kkpacket.PartUint64{})
	data := make([]byte, head.GetSize())
	head.Marshal(data, binary.BigEndian, 1, 2, 3)
	valueList := make([]int, head.GetPartCount())

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := head.UnmarshalTo(data, binary.BigEndian, valueList)
		if err != nil {
			b.Fatalf("unmarshal head: %v", err)
		}
	}
}
