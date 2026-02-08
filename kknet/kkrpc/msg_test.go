package kkrpc

import (
	"fmt"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

func TestAll(t *testing.T) {
	svr := NewServer("localhost:8080", kknet.DefaultOptions())
	err := svr.Start()
	if err != nil {
		t.Fatalf("start server: %v", err)
	}

	cli := NewClient("localhost:8080", kknet.DefaultOptions())
	err = cli.Start()
	if err != nil {
		t.Fatalf("start client: %v", err)
	}

	time.Sleep(2 * time.Second)

	time.Sleep(2 * time.Second)
}

func TestRpcRequest(t *testing.T) {
	request := &Frame{
		T:  FrameTypeRequest,
		ID: 1,
		M:  "test",
		DL: 0,
		P:  []byte("test"),
	}
	fmt.Println(request)

	rows, err := rpcCodec.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	fmt.Println(rows)

	msg := &Frame{}
	err = rpcCodec.Unmarshal(rows, msg)
	if err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	fmt.Println(msg)
	if msg.M != request.M || msg.ID != request.ID || string(msg.P) != string(request.P) {
		t.Fatalf("unmarshal request: %v", err)
	}
}

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
		rows, err := rpcCodec.Marshal(request)
		if err != nil {
			b.Fatalf("marshal request: %v", err)
		}
		msg := Frame{}
		err = rpcCodec.Unmarshal(rows, &msg)
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
		_, err := rpcCodec.Marshal(response)
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
	rows, err := rpcCodec.Marshal(response)
	if err != nil {
		b.Fatalf("marshal response: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msg := Frame{}
		err := rpcCodec.Unmarshal(rows, &msg)
		if err != nil {
			b.Fatalf("unmarshal response: %v", err)
		}
	}
}
