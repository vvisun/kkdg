package testpacket

import (
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/tests/pbmsg"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type msgTest1 struct {
	ID      int
	Data    string
	Name    string
	Age     int
	Email   string
	Phone   string
	Address string
	City    string
	State   string
	Zip     string
	Country string
}

type msgTest2 struct {
	Name    string
	Age     int
	Email   string
	Phone   string
	Address string
	City    string
	State   string
	Zip     string
	Country string
}

func initTestEnv(_ *testing.T) {
	kkpacket.RegisterMsg(1, &msgTest1{}, "test1")
	kkpacket.RegisterMsg(2, &msgTest2{}, "test2")
	kkpacket.RegisterMsg(3, &pbmsg.UserInfo{}, "test3")
}

func TestKK_packet_ProtoBuf(t *testing.T) {
	initTestEnv(t)
	msg := &pbmsg.UserInfo{
		UserId: 1,
		Nick:   "test",
	}
	packet, err := kkpacket.EncodePacket(msg, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeProtoBuf, false))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	t.Logf("packet: %v", packet)
}

func TestKK_packet_Encode(t *testing.T) {
	initTestEnv(t)
	msg1 := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	packet, err := kkpacket.EncodePacket(msg1, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson, false))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	t.Logf("packet: %v", packet)

	packet2, err := kkpacket.EncodePacketEx(msg1, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson, false))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	t.Logf("packet2: %v", packet2)

	msg2, err := kkpacket.DecodePacket[msgTest1](packet, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson, false))
	if err != nil {
		t.Fatalf("decode packet: %v", err)
	}
	t.Logf("msg2: %v", msg2)

	// msg1 == msg2
	if msg1.ID != msg2.ID || msg1.Data != msg2.Data {
		t.Fatalf("msg1 != msg2")
	}
}

func TestKK_packet_EncodeDecode_InvalidPacket(t *testing.T) {
	initTestEnv(t)
	msg1 := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	packet, err := kkpacket.EncodePacket(msg1, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson, false))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	t.Logf("packet: %v", packet)
}

func TestKK_packet_Decode(t *testing.T) {
	initTestEnv(t)
	msg1 := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	packet, err := kkpacket.EncodePacket(msg1, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson, false))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	msg2, err := kkpacket.DecodePacket[msgTest1](packet, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson, false))
	if err != nil {
		t.Fatalf("decode packet: %v", err)
	}
	t.Logf("msg2: %v", msg2)
}

func TestKK_packet_Decode_InvalidPacket(t *testing.T) {
	initTestEnv(t)
	packet := []byte("invalid packet")
	_, err := kkpacket.DecodePacket[msgTest1](packet, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson, false))
	if err == nil {
		t.Fatalf("decode invalid packet should failed")
	}
}
