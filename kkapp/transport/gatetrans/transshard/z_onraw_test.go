package transshard

import (
	"testing"

	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

func newTestShardHandler() *shardHandler {
	router := kkpacket.NewMsgRouter()
	ptotrans.InitShardMsgs(router)
	mp := kkpacket.NewMessagePacket(
		kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
		kkcodec.GetCodec(kkcodec.CodecTypeJson),
		router,
	)
	trans := &transportorShard{
		logicServerMgr:  newLogicServerMgr(),
		sessionMgr:      gatetrans.NewSessionMgr(),
		msgHooker:       gatetrans.NewMsgHooker(),
		transMsgPacket:  mp,
		transStreamTool: kkpacket.DefaultStreamPacket(),
	}
	return &shardHandler{transporter: trans}
}

func encodeTransMsg(t *testing.T, h *shardHandler, msg any) *kkbuffer.ByteBuffer {
	t.Helper()
	bb, err := kkpacket.EncodeStream(msg, h.transporter.transStreamTool, h.transporter.transMsgPacket)
	if err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}
	return bb
}

func corruptBody(t *testing.T, h *shardHandler, bb *kkbuffer.ByteBuffer) {
	t.Helper()
	messageBytes, err := h.transporter.transStreamTool.MessageBytes(bb.B)
	if err != nil {
		t.Fatalf("MessageBytes: %v", err)
	}
	body, err := h.transporter.transMsgPacket.BodyBytes(messageBytes)
	if err != nil {
		t.Fatalf("BodyBytes: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("empty body")
	}
	for i := range body {
		body[i] = 'x'
	}
}

func TestOnRaw_InvalidRegisterBody_DoesNotAddLogicServer(t *testing.T) {
	h := newTestShardHandler()
	bb := encodeTransMsg(t, h, &ptotrans.RpcMsgRegister{
		NodeId:   "logic-1",
		NodeType: "logic",
		ShardIdx: 0,
	})
	corruptBody(t, h, bb)

	h.OnRaw(1, bb)

	if h.transporter.logicServerMgr.getLogicServer("logic-1") != nil {
		t.Fatal("invalid register body must not add logic-1")
	}
	if h.transporter.logicServerMgr.getLogicServer("") != nil {
		t.Fatal("invalid register body must not add empty nodeId")
	}
}

func TestOnRaw_ValidRegister_AddsLogicServer(t *testing.T) {
	h := newTestShardHandler()
	bb := encodeTransMsg(t, h, &ptotrans.RpcMsgRegister{
		NodeId:   "logic-1",
		NodeType: "logic",
		ShardIdx: 0,
	})

	h.OnRaw(1, bb)

	ls := h.transporter.logicServerMgr.getLogicServer("logic-1")
	if ls == nil {
		t.Fatal("valid register must add logic-1")
	}
	if ls.GetNodeType() != "logic" {
		t.Fatalf("nodeType = %q, want logic", ls.GetNodeType())
	}
}

func TestOnRaw_InvalidLoginBody_DoesNotNotify(t *testing.T) {
	h := newTestShardHandler()
	notified := 0
	h.transporter.msgHooker.AddListener(func(msgId kkpacket.MSGID, data any) {
		notified++
	})
	bb := encodeTransMsg(t, h, &ptotrans.RpcClientLoginLogout{
		ClientId: "gate1-1",
		UserId:   7,
		IsLogin:  true,
	})
	corruptBody(t, h, bb)

	h.OnRaw(1, bb)

	if notified != 0 {
		t.Fatalf("invalid login body must not Notify, got %d", notified)
	}
}
