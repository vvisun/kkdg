package testpacket

import (
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/tests/pbmsg"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

func Benchmark_kkpacket_ProtoBuf_Encode(b *testing.B) {
	initTestEnv(nil)
	msg := &pbmsg.UserInfo{
		UserId: 1,
		Nick:   "test",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.EncodePacket(msg, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeProtoBuf, false))
	}
}

func Benchmark_kkpacket_ProtoBuf_Decode(b *testing.B) {
	initTestEnv(nil)
	msg := &pbmsg.UserInfo{
		UserId: 1,
		Nick:   "test",
	}
	packet, err := kkpacket.EncodePacket(msg, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeProtoBuf, false))
	if err != nil {
		b.Fatalf("encode packet: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.DecodePacket[pbmsg.UserInfo](packet, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeProtoBuf, false))
	}
}

func Benchmark_kkpacket_ProtoBuf_EncodeDecode(b *testing.B) {
	initTestEnv(nil)
	msg := &pbmsg.UserInfo{
		UserId: 1,
		Nick:   "test",
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		packet, err := kkpacket.EncodePacketEx(msg, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeProtoBuf, false))
		if err != nil {
			b.Fatalf("encode packet: %v", err)
		}

		_, err = kkpacket.DecodePacket[pbmsg.UserInfo](packet.B, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeProtoBuf, false))
		if err != nil {
			b.Fatalf("decode packet: %v", err)
		}
		kkbuffer.Put(packet)
	}
}
