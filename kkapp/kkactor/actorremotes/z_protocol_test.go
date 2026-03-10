package actorremotes

import (
	"errors"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkerrors"
)

type protoReq struct {
	Value string
}

type protoRsp struct {
	Value string
}

func TestParseTarget(t *testing.T) {
	target, err := ParseTarget("node1/echo_actor")
	if err != nil {
		t.Fatalf("ParseTarget: %v", err)
	}
	if target.NodeID != "node1" || target.ActorKey != "echo_actor" {
		t.Fatalf("target = %+v", target)
	}
	if target.ActorName() != "node1/echo_actor" {
		t.Fatalf("actor name = %q", target.ActorName())
	}

	_, err = ParseTarget("bad-target")
	if err == nil || !errors.Is(err, kkerrors.ErrActorRemoteInvalidTarget) {
		t.Fatalf("ParseTarget invalid err = %v", err)
	}
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
