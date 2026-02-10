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

func TestRouter(t *testing.T) {
	router := NewRpcReceiver()
	RegistRpcHandler(router, "test", func(ctx context.Context, msg *testReq, resp *testRsp) error {
		fmt.Println("remote call testReq", msg)
		resp.Code = 0
		resp.Msg = "success"
		return nil
	})

	msg := &testReq{
		ID:   1,
		Data: "test",
	}
	msgBytes, err := payloadCodec.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal msg: %v", err)
	}
	bb, err := EncodeRpcFrameWithPayload(FrameTypeRequest, 1, "test", msgBytes)
	if err != nil {
		t.Fatalf("encode rpc frame: %v", err)
	}
	router.OnRaw(1, bb, newPendingMap())
}
