package testpacket

import (
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kkpool"
)

func Benchmark_kkpacket_Encode(b *testing.B) {
	initTestEnv(nil)
	msg := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.EncodePacket(msg, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	}
}

func Benchmark_kkpacket_EncodeEx(b *testing.B) {
	initTestEnv(nil)
	msg := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf, _ := kkpacket.EncodePacketEx(msg, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
		kkbuffer.Put(buf)
	}
}

func Benchmark_kkpacket_Decode(b *testing.B) {
	initTestEnv(nil)
	msg := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	packet, err := kkpacket.EncodePacket(msg, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	if err != nil {
		b.Fatalf("encode packet: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.DecodePacket(packet, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
		kkpool.GetFactory[msgTest1]().Put(msg)
	}
}

func Benchmark_kkpacket_EncodeDecode(b *testing.B) {
	initTestEnv(nil)
	msg := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		packet, err := kkpacket.EncodePacketEx(msg, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
		if err != nil {
			b.Fatalf("encode packet: %v", err)
		}
		_, _, err = kkpacket.DecodePacket(packet.B, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
		if err != nil {
			b.Fatalf("decode packet: %v", err)
		}
		kkbuffer.Put(packet)
		kkpool.GetFactory[msgTest1]().Put(msg)
	}
}
