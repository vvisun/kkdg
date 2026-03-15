package kkrpc

import (
	"context"
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/kknet"
)

func TestRouter_ReqRsp(t *testing.T) {
	methodMgr := NewMethodManager(gStreamTool, gFrameCodec, gPayloadCodec)
	RegisterReqRspMethod[testReq, testRsp]("test", methodMgr)
	router := NewRpcReceiver(DefaultRpcOption(), methodMgr)
	RegistReqRspHandler(router, func(ctx context.Context, msg *testReq, resp *testRsp, connId kknet.CONN_ID) error {
		fmt.Println("remote call testReq", msg)
		resp.Code = 0
		resp.Msg = "success"
		return nil
	})

	msg := &testReq{
		ID:   1,
		Data: "test",
	}
	bb, err := EncodeRpcFrame(router.methodMgr, FrameTypeRequest, 1, "test", msg, 0)
	if err != nil {
		t.Fatalf("encode rpc frame: %v", err)
	}
	router.OnRaw(1, bb, newPendingMap(1024))
}

func TestRouter_OneWay(t *testing.T) {
	methodMgr := NewMethodManager(gStreamTool, gFrameCodec, gPayloadCodec)
	RegisterOneWayMethod[testReq]("test", methodMgr)
	router := NewRpcReceiver(DefaultRpcOption(), methodMgr)
	RegistOneWayHandler(router, func(ctx context.Context, msg *testReq, connId kknet.CONN_ID) error {
		fmt.Println("remote call testReq", msg)
		return nil
	})
	msg := &testReq{
		ID:   1,
		Data: "test",
	}
	bb, err := EncodeRpcFrame(router.methodMgr, FrameTypeOneway, 1, "test", msg, 0)
	if err != nil {
		t.Fatalf("encode rpc frame: %v", err)
	}
	router.OnRaw(1, bb, newPendingMap(1024))
}
