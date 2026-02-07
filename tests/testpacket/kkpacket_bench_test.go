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
		kkpacket.EncodePacket(msg, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeJson)), router)
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
		buf, _ := kkpacket.EncodePacket(msg, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeJson)), router)
		kkbuffer.Put(buf)
	}
}

func Benchmark_kkpacket_Decode(b *testing.B) {
	initTestEnv(nil)
	msg := &msgTest1{
		ID:   1,
		Data: "hello",
	}
	packet, err := kkpacket.EncodePacket(msg, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeJson)), router)
	if err != nil {
		b.Fatalf("encode packet: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.DecodePacket(packet.B, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeJson)), router)
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
		packet, err := kkpacket.EncodePacket(msg, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeJson)), router)
		if err != nil {
			b.Fatalf("encode packet: %v", err)
		}
		_, _, err = kkpacket.DecodePacket(packet.B, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeJson)), router)
		if err != nil {
			b.Fatalf("decode packet: %v", err)
		}
		kkbuffer.Put(packet)
		kkpool.GetFactory[msgTest1]().Put(msg)
	}
}
