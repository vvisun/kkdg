package kkrpc

import (
	"fmt"
	"testing"
)

func Test_FrameCodec(t *testing.T) {
	request := &Frame{
		T:  FrameTypeRequest,
		ID: 1,
		M:  "test",
		DL: 0,
		P:  []byte("test"),
	}
	fmt.Println(request)

	rows, err := gFrameCodec.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	fmt.Println(rows)

	msg := &Frame{}
	err = gFrameCodec.Unmarshal(rows, msg)
	if err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	fmt.Println(msg)
	if msg.M != request.M || msg.ID != request.ID || string(msg.P) != string(request.P) {
		t.Fatalf("unmarshal request: %v", err)
	}
}

//----------------------------------------------------------------

// 性能测试
func Benchmark_Marshal_Unmarshal_RpcRequest(b *testing.B) {
	request := &Frame{
		T:  FrameTypeRequest,
		ID: 1,
		M:  "test",
		DL: 0,
		P:  []byte("test"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := gFrameCodec.Marshal(request)
		if err != nil {
			b.Fatalf("marshal request: %v", err)
		}
		msg := Frame{}
		err = gFrameCodec.Unmarshal(rows, &msg)
		if err != nil {
			b.Fatalf("unmarshal request: %v", err)
		}
	}
}

// 性能测试
func Benchmark_Marshal_RpcResponse(b *testing.B) {
	response := &Frame{
		T:  FrameTypeResponse,
		ID: 1,
		P:  []byte("test"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := gFrameCodec.Marshal(response)
		if err != nil {
			b.Fatalf("marshal response: %v", err)
		}
	}
}

// 性能测试
func Benchmark_Unmarshal_RpcResponse(b *testing.B) {
	response := &Frame{
		T:  FrameTypeResponse,
		ID: 1,
		P:  []byte("test"),
	}
	rows, err := gFrameCodec.Marshal(response)
	if err != nil {
		b.Fatalf("marshal response: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msg := Frame{}
		err := gFrameCodec.Unmarshal(rows, &msg)
		if err != nil {
			b.Fatalf("unmarshal response: %v", err)
		}
	}
}
