package gametransshard

import (
	"testing"

	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

type stubClient struct {
	closes int
	sends  int
}

func (s *stubClient) Connect() error                         { return nil }
func (s *stubClient) Close() error                           { s.closes++; return nil }
func (s *stubClient) Addr() string                           { return "" }
func (s *stubClient) Stats() kknet.StatsSnapshot             { return kknet.StatsSnapshot{} }
func (s *stubClient) IsConnected() bool                      { return true }
func (s *stubClient) IsStopped() bool                        { return false }
func (s *stubClient) SendBuffer(buf *kkbuffer.ByteBuffer) error {
	s.sends++
	kkbuffer.Put(buf)
	return nil
}
func (s *stubClient) SendMsg(any) error { return nil }

func TestSnapshotShardClients_CopiesAllSlots(t *testing.T) {
	var src [transport.BackendShardCnt]*gatewayClient
	for i := 0; i < transport.BackendShardCnt; i++ {
		src[i] = &gatewayClient{shardIdx: i}
	}
	dst := snapshotShardClients(src)
	if len(dst) != transport.BackendShardCnt {
		t.Fatalf("len(dst) = %d, want %d (make(..., 0, n)+copy copies nothing)", len(dst), transport.BackendShardCnt)
	}
	for i, c := range dst {
		if c == nil || c.shardIdx != i {
			t.Fatalf("dst[%d] = %+v, want shardIdx=%d", i, c, i)
		}
	}
}

func TestStop_UnregistersOnceAndClosesAll(t *testing.T) {
	router := kkpacket.NewMsgRouter()
	ptotrans.InitShardMsgs(router)
	mp := kkpacket.NewMessagePacket(
		kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
		kkcodec.GetCodec(kkcodec.CodecTypeJson),
		router,
	)
	trans := &transportorShard{
		nodeId:          "logic-1",
		transMsgPacket:  mp,
		transStreamTool: kkpacket.DefaultStreamPacket(),
	}

	stubs := make([]*stubClient, transport.BackendShardCnt)
	for i := 0; i < transport.BackendShardCnt; i++ {
		stubs[i] = &stubClient{}
		trans.conns[i] = &gatewayClient{shardIdx: i, trans: trans, cli: stubs[i]}
	}

	if err := trans.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := trans.Stop(); err != nil {
		t.Fatalf("second Stop: %v", err)
	}

	sends := 0
	closes := 0
	for i, s := range stubs {
		sends += s.sends
		closes += s.closes
		if s.closes != 1 {
			t.Fatalf("shard %d Close count = %d, want 1", i, s.closes)
		}
	}
	if sends != 1 {
		t.Fatalf("Unregister SendBuffer count = %d, want 1", sends)
	}
	if closes != transport.BackendShardCnt {
		t.Fatalf("total Close = %d, want %d", closes, transport.BackendShardCnt)
	}
}
