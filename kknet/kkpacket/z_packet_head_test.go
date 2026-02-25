package kkpacket

import (
	"encoding/binary"
	"fmt"
	"testing"
)

func TestKK_packet_Head_Marshal_Unmarshal(t *testing.T) {
	head := NewPacketHead(&PartUint16{}, &PartUint32{}, &PartUint64{})
	data := make([]byte, head.GetSize())
	head.Marshal(data, binary.BigEndian, 1, 2, 3)
	fmt.Printf("data: %v\n", data)

	valueList, err := head.Unmarshal(data, binary.BigEndian)
	if err != nil {
		t.Fatalf("unmarshal head: %v", err)
	}
	fmt.Printf("valueList: %v\n", valueList)
}
