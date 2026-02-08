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
	router.OnRaw(1, bb)
}
