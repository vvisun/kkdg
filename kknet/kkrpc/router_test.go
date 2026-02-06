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

func TestRouter(t *testing.T) {
	router := NewRouter()
	handler := NewMsgHandler("test", func(ctx context.Context, msg *testMsg) error {
		fmt.Println(msg)
		return nil
	})
	handler1 := NewMsgHandler("test1", func(ctx context.Context, msg *Frame) error {
		fmt.Println(msg)
		return nil
	})
	RegisterHandler(router, handler)
	RegisterHandler(router, handler1)

	msg := &testMsg{
		ID:   1,
		Data: "test",
	}
	msgBytes, err := dataCodec.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal msg: %v", err)
	}
	router.OnMsg(context.Background(), "test", msgBytes)

	frame := &Frame{
		T: FrameTypeRequest,
		M: "test1",
		P: msgBytes,
	}
	frameBytes, err := rpcCodec.Marshal(frame)
	if err != nil {
		t.Fatalf("marshal frame: %v", err)
	}
	router.OnMsg(context.Background(), "test1", frameBytes)
}
