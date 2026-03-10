package actorremotes

import (
	"errors"
	"strings"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
)

type Target struct {
	NodeID    string
	ActorKey  string
	ActorName string
}

type RequestEnvelope struct {
	TargetActorName string
	MessageType     string
	Payload         []byte
	TimeoutMs       int64
}

type ResponseEnvelope struct {
	MessageType string
	Payload     []byte
	Error       string
}

func ParseTarget(targetActorName string) (*Target, error) {
	nodeID, actorKey, found := strings.Cut(targetActorName, kkapp.ActorKeySep_NodeAndActor)
	if !found || nodeID == "" || !kkapp.IsValidActorNodeId(nodeID) || !kkapp.IsValidActorKey(actorKey) {
		return nil, kkerrors.ErrActorRemoteInvalidTarget
	}
	return &Target{
		NodeID:    nodeID,
		ActorKey:  actorKey,
		ActorName: targetActorName,
	}, nil
}

func BuildRequestEnvelope(targetActorName string, msg any, timeout time.Duration) (*RequestEnvelope, error) {
	if _, err := ParseTarget(targetActorName); err != nil {
		return nil, err
	}
	typeName, payload, err := EncodeMessage(msg)
	if err != nil {
		return nil, err
	}
	return &RequestEnvelope{
		TargetActorName: targetActorName,
		MessageType:     typeName,
		Payload:         payload,
		TimeoutMs:       timeout.Milliseconds(),
	}, nil
}

func EncodeRequestEnvelope(targetActorName string, msg any, timeout time.Duration) ([]byte, error) {
	env, err := BuildRequestEnvelope(targetActorName, msg, timeout)
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
	if _, err := ParseTarget(env.TargetActorName); err != nil {
		return nil, nil, err
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

