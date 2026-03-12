package kkrpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/kknet"
)

func TestRouter_ReqRsp(t *testing.T) {
	router := NewRpcReceiver(gFrameCodec, gPayloadCodec)
	RegistReqRspHandler(router, "test", func(ctx context.Context, msg *testReq, resp *testRsp, connId kknet.CONN_ID) error {
		fmt.Println("remote call testReq", msg)
		resp.Code = 0
		resp.Msg = "success"
		return nil
	})

	msg := &testReq{
		ID:   1,
		Data: "test",
	}
	bb, err := EncodeRpcFrame(router.frameCodec, router.payloadCodec, FrameTypeRequest, 1, "test", msg, 0)
	if err != nil {
		t.Fatalf("encode rpc frame: %v", err)
	}
	router.OnRaw(1, bb, newPendingMap())
}

func TestRouter_OneWay(t *testing.T) {
	router := NewRpcReceiver(gFrameCodec, gPayloadCodec)
	RegistOneWayHandler(router, "test", func(ctx context.Context, msg *testReq, connId kknet.CONN_ID) error {
		fmt.Println("remote call testReq", msg)
		return nil
	})
	msg := &testReq{
		ID:   1,
		Data: "test",
	}
	bb, err := EncodeRpcFrame(router.frameCodec, router.payloadCodec, FrameTypeOneway, 1, "test", msg, 0)
	if err != nil {
		t.Fatalf("encode rpc frame: %v", err)
	}
	router.OnRaw(1, bb, newPendingMap())
}
