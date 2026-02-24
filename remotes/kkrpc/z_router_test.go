package kkrpc

import (
	"context"
	"fmt"
	"testing"
)

type testReq struct {
	ID   int
	Data string
}

type testRsp struct {
	Code int
	Msg  string
}

func TestRouter_ReqRsp(t *testing.T) {
	router := NewRpcReceiver()
	RegistReqRspHandler(router, "test", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		fmt.Println("remote call testReq", msg)
		resp.Code = 0
		resp.Msg = "success"
		return nil
	})

	msg := &testReq{
		ID:   1,
		Data: "test",
	}
	bb, err := EncodeRpcFrame(FrameTypeRequest, 1, "test", msg, 0)
	if err != nil {
		t.Fatalf("encode rpc frame: %v", err)
	}
	router.OnRaw(1, bb, newPendingMap())
}

func TestRouter_OneWay(t *testing.T) {
	router := NewRpcReceiver()
	RegistOneWayHandler(router, "test", func(ctx context.Context, msg *testReq) error {
		fmt.Println("remote call testReq", msg)
		return nil
	})
	msg := &testReq{
		ID:   1,
		Data: "test",
	}
	bb, err := EncodeRpcFrame(FrameTypeOneway, 1, "test", msg, 0)
	if err != nil {
		t.Fatalf("encode rpc frame: %v", err)
	}
	router.OnRaw(1, bb, newPendingMap())
}
