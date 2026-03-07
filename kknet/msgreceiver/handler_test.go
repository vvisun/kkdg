package msgreceiver

import (
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type testMsg struct {
	ID   int
	Data string
}

func TestMsgHandler_OnRaw(t *testing.T) {
	handler := newMsgHandler[testMsg](1, kkcodec.GetCodec(kkcodec.CodecTypeJson), func(connId kknet.CONN_ID, msg *testMsg) error {
		fmt.Println(msg)
		return nil
	})

	bodyBytes, err := kkcodec.GetCodec(kkcodec.CodecTypeJson).Marshal(&testMsg{
		ID:   1,
		Data: "test",
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	err = handler.OnRaw(1, bodyBytes)
	if err != nil {
		t.Fatalf("on raw: %v", err)
	}
}

// receiver test
func TestMsgReceiver_OnRaw(t *testing.T) {
	router := kkpacket.NewMsgRouter()
	router.Register(1, &testMsg{}, "test")
	codec := kkcodec.GetCodec(kkcodec.CodecTypeJson)
	msgPacket := kkpacket.NewMessagePacket(kkpacket.NewPacketHead(&kkpacket.PartUint32{}), codec, router)
	receiver := NewMsgReceiver[kknet.CONN_ID](msgPacket)
	RegisterMsgHandler(receiver, func(connId kknet.CONN_ID, msg *testMsg) error {
		fmt.Println(msg)
		return nil
	})

	stream := kkpacket.NewLengthFieldStreamPacket(4)
	bb, err := kkpacket.EncodeStream(&testMsg{
		ID:   1,
		Data: "test",
	}, stream, msgPacket)
	if err != nil {
		t.Fatalf("encode stream: %v", err)
	}
	receiver.OnRaw(1, bb)
}

func TestMsgReceiver_OnRawWithParser(t *testing.T) {
	router := kkpacket.NewMsgRouter()
	router.Register(1, &testMsg{}, "test")
	codec := kkcodec.GetCodec(kkcodec.CodecTypeJson)
	msgPacket := kkpacket.NewMessagePacket(kkpacket.NewPacketHead(&kkpacket.PartUint32{}), codec, router)
	receiver := NewMsgReceiverWithParser[kknet.CONN_ID](msgPacket, func(data []byte) (kkpacket.MSGID, []byte, error) {
		return 1, data, nil
	})
	RegisterMsgHandler(receiver, func(connId kknet.CONN_ID, msg *testMsg) error {
		fmt.Println(msg)
		return nil
	})

	mbytes, _ := codec.Marshal(&testMsg{
		ID:   1,
		Data: "test",
	})
	bb := kkbuffer.GetWithCapacity(len(mbytes))
	bb.WriteBytes(mbytes)
	receiver.OnRaw(1, bb)
}

func BenchmarkMsgReceiver_OnRaw(b *testing.B) {
	router := kkpacket.NewMsgRouter()
	router.Register(1, &testMsg{}, "test")
	codec := kkcodec.GetCodec(kkcodec.CodecTypeJson)
	msgPacket := kkpacket.NewMessagePacket(kkpacket.NewPacketHead(&kkpacket.PartUint32{}), codec, router)
	receiver := NewMsgReceiver[kknet.CONN_ID](msgPacket)
	RegisterMsgHandler(receiver, func(connId kknet.CONN_ID, msg *testMsg) error {
		// fmt.Println(msg)
		return nil
	})

	stream := kkpacket.NewLengthFieldStreamPacket(4)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msg := testMsg{
			ID:   1,
			Data: "test",
		}
		bb, err := kkpacket.EncodeStream(&msg, stream, msgPacket)
		if err != nil {
			b.Fatalf("encode stream: %v", err)
		}

		receiver.OnRaw(1, bb)
	}
}
