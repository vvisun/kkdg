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

func BuildRequestEnvelope(registry *MessageRegistry, target ActorRef, msg any, timeout time.Duration) (*RequestEnvelope, error) {
	if !target.IsValid() {
		return nil, kkerrors.ErrActorRemoteInvalidTarget
	}
	typeName, payload, err := EncodeMessage(registry, msg)
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

func EncodeRequestEnvelope(registry *MessageRegistry, target ActorRef, msg any, timeout time.Duration) ([]byte, error) {
	env, err := BuildRequestEnvelope(registry, target, msg, timeout)
	if err != nil {
		return nil, err
	}
	return registry.codec.Marshal(env)
}

func DecodeRequestEnvelope(registry *MessageRegistry, data []byte) (*RequestEnvelope, any, error) {
	var env RequestEnvelope
	if err := registry.codec.Unmarshal(data, &env); err != nil {
		return nil, nil, err
	}
	if !env.Target.IsValid() {
		return nil, nil, kkerrors.ErrActorRemoteInvalidTarget
	}
	msg, err := DecodeMessage(registry, env.MessageType, env.Payload)
	if err != nil {
		return nil, nil, err
	}
	return &env, msg, nil
}

func EncodeResponseEnvelope(registry *MessageRegistry, result any, callErr error) ([]byte, error) {
	resp := ResponseEnvelope{}
	if callErr != nil {
		resp.Error = callErr.Error()
		return registry.codec.Marshal(&resp)
	}
	if result == nil {
		return registry.codec.Marshal(&resp)
	}
	typeName, payload, err := EncodeMessage(registry, result)
	if err != nil {
		return nil, err
	}
	resp.MessageType = typeName
	resp.Payload = payload
	return registry.codec.Marshal(&resp)
}

func DecodeResponseEnvelope(registry *MessageRegistry, data []byte) (any, error) {
	var resp ResponseEnvelope
	if err := registry.codec.Unmarshal(data, &resp); err != nil {
		return nil, err
	}
	if resp.Error != "" {
		return nil, errors.New(resp.Error)
	}
	if resp.MessageType == "" && len(resp.Payload) == 0 {
		return nil, nil
	}
	return DecodeMessage(registry, resp.MessageType, resp.Payload)
}
