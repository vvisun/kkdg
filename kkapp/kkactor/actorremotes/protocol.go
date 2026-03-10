package actorremotes

import (
	"errors"
	"strings"
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

func ParseTarget(targetActorName string) (*ActorRef, error) {
	nodeID, actorKey, found := strings.Cut(targetActorName, kkapp.ActorKeySep_NodeAndActor)
	if !found || nodeID == "" || !kkapp.IsValidActorNodeId(nodeID) || !kkapp.IsValidActorKey(actorKey) {
		return nil, kkerrors.ErrActorRemoteInvalidTarget
	}
	return &ActorRef{
		NodeID:   nodeID,
		ActorKey: actorKey,
	}, nil
}

func (ref ActorRef) IsValid() bool {
	return ref.NodeID != "" && kkapp.IsValidActorNodeId(ref.NodeID) && kkapp.IsValidActorKey(ref.ActorKey)
}

func (ref ActorRef) ActorName() string {
	if !ref.IsValid() {
		return ""
	}
	return ref.NodeID + kkapp.ActorKeySep_NodeAndActor + ref.ActorKey
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
	return DecodeMessage(resp.MessageType, resp.Payload)
}

