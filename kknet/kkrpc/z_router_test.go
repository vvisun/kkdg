package kkrpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type testMsg struct {
	ID   int
	Data string
}

type testRsp struct {
	Code int
	Msg  string
}

func TestRouter(t *testing.T) {
	router := NewRpcRouter()
	RegistRpcHandler(router, "test", func(ctx context.Context, msg *testMsg) error {
		fmt.Println(msg)
		return nil
	})

	msg := &testMsg{
		ID:   1,
		Data: "test",
	}
	bb, err := EncodeRpcFrame(FrameTypeRequest, 1, "test", msg)
	if err != nil {
		t.Fatalf("encode rpc frame: %v", err)
	}
	router.OnMsg(context.Background(), "test", bb.Bytes())
}

func TestMsgPeer_Request(t *testing.T) {
	peer := NewMsgPeer[testMsg, testRsp]("test")
	bb, err := peer.EncodeReq(&testMsg{
		ID:   1,
		Data: "test",
	})
	if err != nil {
		t.Fatalf("encode req: %v", err)
	}

	req, err := peer.DecodeReq(bb)
	if err != nil {
		t.Fatalf("decode req: %v", err)
	}
	fmt.Println(req, req.ID, req.Data)
	if req.ID != 1 || req.Data != "test" {
		t.Fatalf("req != testMsg{ID: 1, Data: 'test'}")
	}
}

func TestMsgPeer_Response(t *testing.T) {
	peer := NewMsgPeer[testMsg, testRsp]("test")
	bb, err := peer.EncodeRsp(&testRsp{
		Code: 0,
		Msg:  "test",
	})
	if err != nil {
		t.Fatalf("encode rsp: %v", err)
	}
	rsp, err := peer.DecodeRsp(bb)
	if err != nil {
		t.Fatalf("decode rsp: %v", err)
	}
	fmt.Println(rsp, rsp.Code, rsp.Msg)
	if rsp.Code != 0 || rsp.Msg != "test" {
		t.Fatalf("rsp != testRsp{Code: 0, Msg: 'test'}")
	}
}

func TestMsgPeer_Request_Response(t *testing.T) {
	peer := NewMsgPeer[testMsg, testRsp]("test")
	bb, err := peer.EncodeReq(&testMsg{
		ID:   1,
		Data: "test",
	})
	if err != nil {
		t.Fatalf("encode req: %v", err)
	}
	req, err := peer.DecodeReq(bb)
	if err != nil {
		t.Fatalf("decode req: %v", err)
	}
	fmt.Println(req, req.ID, req.Data)
	if req.ID != 1 || req.Data != "test" {
		t.Fatalf("req != testMsg{ID: 1, Data: 'test'}")
	}

	bb, err = peer.EncodeRsp(&testRsp{
		Code: 0,
		Msg:  "test",
	})
	if err != nil {
		t.Fatalf("encode rsp: %v", err)
	}
	rsp, err := peer.DecodeRsp(bb)
	if err != nil {
		t.Fatalf("decode rsp: %v", err)
	}
	fmt.Println(rsp, rsp.Code, rsp.Msg)
	if rsp.Code != 0 || rsp.Msg != "test" {
		t.Fatalf("rsp != testRsp{Code: 0, Msg: 'test'}")
	}
}

func BenchmarkMsgPeer_Request_Response(b *testing.B) {
	peer := NewMsgPeer[testMsg, testRsp]("test")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb, err := peer.EncodeReq(&testMsg{
			ID:   1,
			Data: "test",
		})
		if err != nil {
			b.Fatalf("encode req: %v", err)
		}
		req, err := peer.DecodeReq(bb)
		if err != nil {
			b.Fatalf("decode req: %v", err)
		}
		if req.ID != 1 || req.Data != "test" {
			b.Fatalf("req != testMsg{ID: 1, Data: 'test'}")
		}
		kkbuffer.Put(bb)

		bb, err = peer.EncodeRsp(&testRsp{
			Code: 0,
			Msg:  "test",
		})
		if err != nil {
			b.Fatalf("encode rsp: %v", err)
		}
		rsp, err := peer.DecodeRsp(bb)
		if err != nil {
			b.Fatalf("decode rsp: %v", err)
		}
		if rsp.Code != 0 || rsp.Msg != "test" {
			b.Fatalf("rsp != testRsp{Code: 0, Msg: 'test'}")
		}
		kkbuffer.Put(bb)
	}
}

func BenchmarkMsgPeer_Encode(b *testing.B) {
	peer := NewMsgPeer[testMsg, testRsp]("test")
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bb, err := peer.EncodeReq(&testMsg{
			ID:   1,
			Data: "test",
		})
		if err != nil {
			b.Fatalf("encode req: %v", err)
		}
		kkbuffer.Put(bb)
	}
}

func BenchmarkMsgPeer_Decode(b *testing.B) {
	peer := NewMsgPeer[testMsg, testRsp]("test")

	bb, err := peer.EncodeReq(&testMsg{
		ID:   1,
		Data: "test",
	})
	if err != nil {
		b.Fatalf("encode req: %v", err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		req, err := peer.DecodeReq(bb)
		if err != nil {
			b.Fatalf("decode req: %v", err)
		}
		if req.ID != 1 || req.Data != "test" {
			b.Fatalf("req != testMsg{ID: 1, Data: 'test'}")
		}
	}
}
