package actorremotes

import (
	"errors"
	"testing"
	"time"
)

type protoReq struct {
	Value string
}

type protoRsp struct {
	Value string
}

func TestEncodeDecodeRequestEnvelope(t *testing.T) {
	if err := RegisterMessage(&protoReq{}); err != nil {
		t.Fatalf("RegisterMessage req: %v", err)
	}

	data, err := EncodeRequestEnvelope(ActorRef{NodeID: "node1", ActorKey: "echo_actor"}, &protoReq{Value: "hello"}, 3*time.Second)
	if err != nil {
		t.Fatalf("EncodeRequestEnvelope: %v", err)
	}

	env, msg, err := DecodeRequestEnvelope(data)
	if err != nil {
		t.Fatalf("DecodeRequestEnvelope: %v", err)
	}
	if env.Target.NodeID != "node1" || env.Target.ActorKey != "echo_actor" {
		t.Fatalf("target = %+v", env.Target)
	}
	if env.TimeoutMs != int64((3 * time.Second).Milliseconds()) {
		t.Fatalf("timeout = %d", env.TimeoutMs)
	}
	req, ok := msg.(*protoReq)
	if !ok || req.Value != "hello" {
		t.Fatalf("msg = %#v", msg)
	}
}

func TestEncodeDecodeResponseEnvelope(t *testing.T) {
	if err := RegisterMessage(&protoRsp{}); err != nil {
		t.Fatalf("RegisterMessage rsp: %v", err)
	}

	data, err := EncodeResponseEnvelope(&protoRsp{Value: "world"}, nil)
	if err != nil {
		t.Fatalf("EncodeResponseEnvelope: %v", err)
	}
	msg, err := DecodeResponseEnvelope(data)
	if err != nil {
		t.Fatalf("DecodeResponseEnvelope: %v", err)
	}
	rsp, ok := msg.(*protoRsp)
	if !ok || rsp.Value != "world" {
		t.Fatalf("msg = %#v", msg)
	}

	data, err = EncodeResponseEnvelope(nil, errors.New("boom"))
	if err != nil {
		t.Fatalf("EncodeResponseEnvelope error: %v", err)
	}
	_, err = DecodeResponseEnvelope(data)
	if err == nil || err.Error() != "boom" {
		t.Fatalf("DecodeResponseEnvelope err = %v", err)
	}
}

func TestEncodeDecodeResponseEnvelope_NilResult(t *testing.T) {
	data, err := EncodeResponseEnvelope(nil, nil)
	if err != nil {
		t.Fatalf("EncodeResponseEnvelope(nil, nil): %v", err)
	}

	msg, err := DecodeResponseEnvelope(data)
	if err != nil {
		t.Fatalf("DecodeResponseEnvelope(nil result): %v", err)
	}
	if msg != nil {
		t.Fatalf("msg = %#v, want nil", msg)
	}
}

// ----------------------------------------------------------------
// Benchmarks

func BenchmarkEncodeRequestEnvelope(b *testing.B) {
	if err := RegisterMessage(&protoReq{}); err != nil {
		b.Fatalf("RegisterMessage req: %v", err)
	}
	ref := ActorRef{NodeID: "node1", ActorKey: "echo_actor"}
	msg := &protoReq{Value: "hello"}
	timeout := 3 * time.Second

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeRequestEnvelope(ref, msg, timeout); err != nil {
			b.Fatalf("EncodeRequestEnvelope: %v", err)
		}
	}
}

func BenchmarkDecodeRequestEnvelope(b *testing.B) {
	if err := RegisterMessage(&protoReq{}); err != nil {
		b.Fatalf("RegisterMessage req: %v", err)
	}
	ref := ActorRef{NodeID: "node1", ActorKey: "echo_actor"}
	msg := &protoReq{Value: "hello"}
	timeout := 3 * time.Second

	encoded, err := EncodeRequestEnvelope(ref, msg, timeout)
	if err != nil {
		b.Fatalf("EncodeRequestEnvelope: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DecodeRequestEnvelope(encoded); err != nil {
			b.Fatalf("DecodeRequestEnvelope: %v", err)
		}
	}
}

func BenchmarkEncodeResponseEnvelope_Success(b *testing.B) {
	if err := RegisterMessage(&protoRsp{}); err != nil {
		b.Fatalf("RegisterMessage rsp: %v", err)
	}
	result := &protoRsp{Value: "world"}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeResponseEnvelope(result, nil); err != nil {
			b.Fatalf("EncodeResponseEnvelope: %v", err)
		}
	}
}

func BenchmarkEncodeResponseEnvelope_Error(b *testing.B) {
	callErr := errors.New("boom")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := EncodeResponseEnvelope(nil, callErr); err != nil {
			b.Fatalf("EncodeResponseEnvelope: %v", err)
		}
	}
}

func BenchmarkDecodeResponseEnvelope_Success(b *testing.B) {
	if err := RegisterMessage(&protoRsp{}); err != nil {
		b.Fatalf("RegisterMessage rsp: %v", err)
	}
	result := &protoRsp{Value: "world"}
	encoded, err := EncodeResponseEnvelope(result, nil)
	if err != nil {
		b.Fatalf("EncodeResponseEnvelope: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := DecodeResponseEnvelope(encoded); err != nil {
			b.Fatalf("DecodeResponseEnvelope: %v", err)
		}
	}
}

