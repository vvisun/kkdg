package msgreceiver

import (
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type testMsg struct {
	ID       int
	StrData  string
	ByteList []byte
}

func newTestMsg() *testMsg {
	msg := &testMsg{
		ID:      1,
		StrData: "testaaaaaaaaatestaaaaaaaaatestaaaaaaaaatesaaaaaaa",
	}
	msg.ByteList = make([]byte, 656)
	return msg
}

func TestMsgHandler_OnRaw(t *testing.T) {
	handler := newMsgHandler(1, kkcodec.GetCodec(kkcodec.CodecTypeJson), func(connId kknet.CONN_ID, msg *testMsg) {
		fmt.Println(msg)
	})

	bodyBytes, err := kkcodec.GetCodec(kkcodec.CodecTypeJson).Marshal(newTestMsg())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	err = handler.OnMessage(1, bodyBytes, false)
	if err != nil {
		t.Fatalf("on raw: %v", err)
	}
}

// receiver test
func TestMsgReceiver_OnRaw(t *testing.T) {
	router := kkpacket.NewMsgRouter()
	router.Register(1, &testMsg{}, "test")
	codec := kkcodec.GetCodec(kkcodec.CodecTypeJson)
	messageTool := kkpacket.NewMessagePacket(kkpacket.NewPacketHead(&kkpacket.PartUint32{}), codec, router)
	streamTool := kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
	packetTool := kkpacket.NewFullPacket(streamTool, messageTool)
	receiver := NewMsgReceiver(packetTool, nil)
	RegisterMsgHandler(receiver, func(connId kknet.CONN_ID, msg *testMsg) {
		fmt.Println(msg)
	})

	stream := kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
	bb, err := kkpacket.EncodeStream(newTestMsg(), stream, messageTool)
	if err != nil {
		t.Fatalf("encode stream: %v", err)
	}
	receiver.OnRaw(1, bb)
}

func TestSessionMsgReceiver_ThreadIdxGuard(t *testing.T) {
	router := kkpacket.NewMsgRouter()
	router.Register(1, &testMsg{}, "test")
	codec := kkcodec.GetCodec(kkcodec.CodecTypeJson)
	messageTool := kkpacket.NewMessagePacket(kkpacket.NewPacketHead(&kkpacket.PartUint32{}), codec, router)
	streamTool := kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
	packetTool := kkpacket.NewFullPacket(streamTool, messageTool)
	mgr := gametrans.NewSessionManager(2)
	recv := NewSessionMsgReceiver(packetTool, mgr, nil)
	if recv.ThreadWorkerCount() != 2 {
		t.Fatalf("ThreadWorkerCount = %d, want 2", recv.ThreadWorkerCount())
	}
	if err := gametrans.CheckReceiverWorkers(mgr, recv); err != nil {
		t.Fatalf("startup check: %v", err)
	}

	bb, err := kkpacket.EncodeStream(newTestMsg(), streamTool, messageTool)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	recv.OnSession("s1", bb.B, -1)
	recv.OnSession("s1", bb.B, 2)
}

func BenchmarkMsgReceiver_OnRaw(b *testing.B) {
	router := kkpacket.NewMsgRouter()
	router.Register(1, &testMsg{}, "test")
	codec := kkcodec.GetCodec(kkcodec.CodecTypeJson)
	messageTool := kkpacket.NewMessagePacket(kkpacket.NewPacketHead(&kkpacket.PartUint32{}), codec, router)
	streamTool := kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
	packetTool := kkpacket.NewFullPacket(streamTool, messageTool)
	receiver := NewMsgReceiver(packetTool, nil)
	RegisterMsgHandler(receiver, func(connId kknet.CONN_ID, msg *testMsg) {
		// fmt.Println(msg)
	})

	stream := kkpacket.NewLengthFieldStreamPacket(4, 4*1024)
	msg := newTestMsg()
	bb, err := kkpacket.EncodeStream(msg, stream, messageTool)
	if err != nil {
		b.Fatalf("encode stream: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bbCopy := kkbuffer.GetWithLenCap(len(bb.B), len(bb.B))
		copy(bbCopy.B, bb.B)
		receiver.OnRaw(1, bbCopy)
	}
}
