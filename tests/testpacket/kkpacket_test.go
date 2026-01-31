package testpacket

import (
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/proto/pbcluster"
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
	kkpacket.RegisterMsg(3, &pbcluster.ClusterPacket{}, "test3")
}

func TestKK_packet_ProtoBuf(t *testing.T) {
	initTestEnv(t)
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
	packet, err := kkpacket.EncodePacket(msg, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeProtoBuf))
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
	packet, err := kkpacket.EncodePacket(msg1, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	t.Logf("packet: %v", packet)

	packet2, err := kkpacket.EncodePacketEx(msg1, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	t.Logf("packet2: %v", packet2)

	msgAA, errAA := kkpacket.DecodePacket(packet[:len(packet)-5], kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	if errAA == nil {
		fmt.Printf("msgAA: %+v\n", msgAA)
	} else {
		fmt.Printf("errAA: %+v\n", errAA)
	}

	msg22, err := kkpacket.DecodePacket(packet, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	if err != nil {
		t.Fatalf("decode packet: %v", err)
	}
	msg2, ok := msg22.(*msgTest1)
	if !ok {
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
	packet, err := kkpacket.EncodePacket(msg1, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
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
	packet, err := kkpacket.EncodePacket(msg1, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}
	msg22, err := kkpacket.DecodePacket(packet, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	if err != nil {
		t.Fatalf("decode packet: %v", err)
	}
	msg2, ok := msg22.(*msgTest1)
	if !ok {
		t.Fatalf("decode packet: %v", err)
	}
	t.Logf("msg2: %v", msg2)
}

func TestKK_packet_Decode_InvalidPacket(t *testing.T) {
	initTestEnv(t)
	packet := []byte("invalid packet")
	_, err := kkpacket.DecodePacket(packet, kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	if err == nil {
		t.Fatalf("decode invalid packet should failed")
	}
}

func TestKK_packet_EncodeDecode_Stream(t *testing.T) {
	initTestEnv(t)
	msg1 := &pbcluster.ClusterPacket{
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
	packet, err := kkpacket.EncodeStream(msg1, kkpacket.DefaultStreamPacket())
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}

	msg22, err := kkpacket.DecodeStream(packet.B, kkpacket.DefaultStreamPacket())
	if err != nil {
		t.Fatalf("decode packet: %v", err)
	}
	msg2, ok := msg22.(*pbcluster.ClusterPacket)
	if !ok {
		t.Fatalf("decode packet: %v", err)
	}
	t.Logf("msg2: %v", msg2)

	if msg1.BuildTime != msg2.BuildTime || msg1.Timeout != msg2.Timeout || msg1.SourcePath != msg2.SourcePath || msg1.TargetPath != msg2.TargetPath || msg1.FuncName != msg2.FuncName || msg1.Session.Uid != msg2.Session.Uid || msg1.Session.AgentPath != msg2.Session.AgentPath || msg1.Session.Ip != msg2.Session.Ip {
		t.Fatalf("msg1 != msg2")
	}
}

func TestKK_packet_EncodeDecode_StreamJson(t *testing.T) {
	initTestEnv(t)
	stream := kkpacket.NewLengthFieldStreamPacket(kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeJson))
	msg1 := &pbcluster.ClusterPacket{
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
	packet, err := kkpacket.EncodeStream(msg1, stream)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}

	msg22, err := kkpacket.DecodeStream(packet.B, stream)
	if err != nil {
		t.Fatalf("decode packet: %v", err)
	}
	msg2, ok := msg22.(*pbcluster.ClusterPacket)
	if !ok {
		t.Fatalf("decode packet: %v", err)
	}
	t.Logf("msg2: %v", msg2)

	if msg1.BuildTime != msg2.BuildTime || msg1.Timeout != msg2.Timeout || msg1.SourcePath != msg2.SourcePath || msg1.TargetPath != msg2.TargetPath || msg1.FuncName != msg2.FuncName || msg1.Session.Uid != msg2.Session.Uid || msg1.Session.AgentPath != msg2.Session.AgentPath || msg1.Session.Ip != msg2.Session.Ip {
		t.Fatalf("msg1 != msg2")
	}
}

func TestKK_packet_EncodeDecode_StreamMsgpack(t *testing.T) {
	initTestEnv(t)
	stream := kkpacket.NewLengthFieldStreamPacket(kkpacket.NewPacker(kkpacket.HeadTypeMid, kkcodec.CodecTypeMsgpack))
	msg1 := &pbcluster.ClusterPacket{
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
	packet, err := kkpacket.EncodeStream(msg1, stream)
	if err != nil {
		t.Fatalf("encode packet: %v", err)
	}

	msg22, err := kkpacket.DecodeStream(packet.B, stream)
	if err != nil {
		t.Fatalf("decode packet: %v", err)
	}
	msg2, ok := msg22.(*pbcluster.ClusterPacket)
	if !ok {
		t.Fatalf("decode packet: %v", err)
	}
	t.Logf("msg2: %v", msg2)

	if msg1.BuildTime != msg2.BuildTime || msg1.Timeout != msg2.Timeout || msg1.SourcePath != msg2.SourcePath || msg1.TargetPath != msg2.TargetPath || msg1.FuncName != msg2.FuncName || msg1.Session.Uid != msg2.Session.Uid || msg1.Session.AgentPath != msg2.Session.AgentPath || msg1.Session.Ip != msg2.Session.Ip {
		t.Fatalf("msg1 != msg2")
	}
}
