package kkrpc

import (
	"context"
	"fmt"
	"testing"
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
	RegistRpcHandler(router, "test", func(ctx context.Context, msg *testMsg, resp *testRsp) error {
		fmt.Println(msg)
		resp.Code = 0
		resp.Msg = "success"
		return nil
	})

	msg := &testMsg{
		ID:   1,
		Data: "test",
	}
	msgBytes, err := dataCodec.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal msg: %v", err)
	}
	bb, err := EncodeRpcFrame(FrameTypeRequest, 1, "test", msgBytes)
	if err != nil {
		t.Fatalf("encode rpc frame: %v", err)
	}
	router.OnRaw(1, bb)
}
