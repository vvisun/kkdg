package kkrpc

import (
	"fmt"
	"testing"
)

func TestRpcRequest(t *testing.T) {
	request := &RpcRequest{
		Method: "test",
		ReqId:  1,
		Data:   []byte("test"),
	}
	fmt.Println(request)

	rows, err := rpcCodec.Marshal(request)
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	fmt.Println(rows)

	msg := &RpcRequest{}
	err = rpcCodec.Unmarshal(rows, msg)
	if err != nil {
		t.Fatalf("unmarshal request: %v", err)
	}
	fmt.Println(msg)
	if msg.Method != request.Method || msg.ReqId != request.ReqId || string(msg.Data) != string(request.Data) {
		t.Fatalf("unmarshal request: %v", err)
	}
}

// 性能测试
func Benchmark_Marshal_Unmarshal_RpcRequest(b *testing.B) {
	request := &RpcRequest{
		Method: "test",
		ReqId:  1,
		Data:   []byte("test"),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		rows, err := rpcCodec.Marshal(request)
		if err != nil {
			b.Fatalf("marshal request: %v", err)
		}
		msg := RpcRequest{}
		err = rpcCodec.Unmarshal(rows, &msg)
		if err != nil {
			b.Fatalf("unmarshal request: %v", err)
		}
	}
}

// 性能测试
func Benchmark_Marshal_RpcResponse(b *testing.B) {
	response := &RpcResponse{
		ReqId: 1,
		Data:  []byte("test"),
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
	response := &RpcResponse{
		ReqId: 1,
		Data:  []byte("test"),
	}
	rows, err := rpcCodec.Marshal(response)
	if err != nil {
		b.Fatalf("marshal response: %v", err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		msg := RpcResponse{}
		err := rpcCodec.Unmarshal(rows, &msg)
		if err != nil {
			b.Fatalf("unmarshal response: %v", err)
		}
	}
}
