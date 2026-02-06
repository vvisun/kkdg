package testpacket

import (
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/proto/pbcluster"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

func Benchmark_kkpacket_ProtoBuf_Encode(b *testing.B) {
	initTestEnv(nil)
	msg := &pbcluster.ClusterPacket{
		BuildTime:  1,
		Timeout:    1,
		SourcePath: "test",
		TargetPath: "test",
		FuncName:   "test",
		ArgBytes:   []byte("test"),
		Session: &pbcluster.Session{
			Sid:       "test",
			Uid:       1,
			AgentPath: "test",
			Ip:        "127.0.0.1",
			ExtendData: map[string]string{
				"test": "test",
			},
		},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.EncodePacket(msg, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeProtoBuf)), router)
	}
}

func Benchmark_kkpacket_ProtoBuf_Decode(b *testing.B) {
	initTestEnv(nil)
	msg := &pbcluster.ClusterPacket{
		BuildTime:  1,
		Timeout:    1,
		SourcePath: "test",
		TargetPath: "test",
		FuncName:   "test",
		ArgBytes:   []byte("test"),
		Session: &pbcluster.Session{
			Sid:       "test",
			Uid:       1,
			AgentPath: "test",
			Ip:        "127.0.0.1",
			ExtendData: map[string]string{
				"test": "test",
			},
		},
	}
	packet, err := kkpacket.EncodePacket(msg, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeProtoBuf)), router)
	if err != nil {
		b.Fatalf("encode packet: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		kkpacket.DecodePacket(packet, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeProtoBuf)), router)
	}
}

func Benchmark_kkpacket_ProtoBuf_EncodeDecode(b *testing.B) {
	initTestEnv(nil)
	msg := &pbcluster.ClusterPacket{
		BuildTime:  1,
		Timeout:    1,
		SourcePath: "test",
		TargetPath: "test",
		FuncName:   "test",
		ArgBytes:   []byte("test"),
		Session: &pbcluster.Session{
			Sid:       "test",
			Uid:       1,
			AgentPath: "test",
			Ip:        "127.0.0.1",
			ExtendData: map[string]string{
				"test": "test",
			},
		},
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		packet, err := kkpacket.EncodePacketEx(msg, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeProtoBuf)), router)
		if err != nil {
			b.Fatalf("encode packet: %v", err)
		}

		_, _, err = kkpacket.DecodePacket(packet.B, kkpacket.NewPacketCodec(kkpacket.HeadTypeMid, kkcodec.GetCodec(kkcodec.CodecTypeProtoBuf)), router)
		if err != nil {
			b.Fatalf("decode packet: %v", err)
		}
		kkbuffer.Put(packet)
	}
}
