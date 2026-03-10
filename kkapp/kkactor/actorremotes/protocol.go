package actorremotes

import (
	"errors"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
)

type ActorRef struct {
	NodeID   string
	ActorKey string
}

type RequestEnvelope struct {
	Target      ActorRef
	MessageType string
	Payload     []byte
	TimeoutMs   int64
}

type ResponseEnvelope struct {
	MessageType string
	Payload     []byte
	Error       string
}

// IsValid 校验远程目标是否合法。
// 远程协议要求 NodeID 非空；空 NodeID 仅用于进程内本地 actor 标识。
func (ref ActorRef) IsValid() bool {
	return ref.NodeID != "" && kkapp.IsValidActorNodeId(ref.NodeID) && kkapp.IsValidActorKey(ref.ActorKey)
}

func BuildRequestEnvelope(target ActorRef, msg any, timeout time.Duration) (*RequestEnvelope, error) {
	if !target.IsValid() {
		return nil, kkerrors.ErrActorRemoteInvalidTarget
	}
	typeName, payload, err := EncodeMessage(msg)
	if err != nil {
		return nil, err
	}
	return &RequestEnvelope{
		Target:      target,
		MessageType: typeName,
		Payload:     payload,
		TimeoutMs:   timeout.Milliseconds(),
	}, nil
}

func EncodeRequestEnvelope(target ActorRef, msg any, timeout time.Duration) ([]byte, error) {
	env, err := BuildRequestEnvelope(target, msg, timeout)
	if err != nil {
		return nil, err
	}
	return msgCodec.Marshal(env)
}

func DecodeRequestEnvelope(data []byte) (*RequestEnvelope, any, error) {
	var env RequestEnvelope
	if err := msgCodec.Unmarshal(data, &env); err != nil {
		return nil, nil, err
	}
	if !env.Target.IsValid() {
		return nil, nil, kkerrors.ErrActorRemoteInvalidTarget
	}
	msg, err := DecodeMessage(env.MessageType, env.Payload)
	if err != nil {
		return nil, nil, err
	}
	return &env, msg, nil
}

func EncodeResponseEnvelope(result any, callErr error) ([]byte, error) {
	resp := ResponseEnvelope{}
	if callErr != nil {
		resp.Error = callErr.Error()
		return msgCodec.Marshal(&resp)
	}
	if result == nil {
		return msgCodec.Marshal(&resp)
	}
	typeName, payload, err := EncodeMessage(result)
	if err != nil {
		return nil, err
	}
	resp.MessageType = typeName
	resp.Payload = payload
	return msgCodec.Marshal(&resp)
}

func DecodeResponseEnvelope(data []byte) (any, error) {
	var resp ResponseEnvelope
	if err := msgCodec.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	if resp.MessageType == "" && len(resp.Payload) == 0 {
		return nil, nil
	}
	return DecodeMessage(resp.MessageType, resp.Payload)
}
