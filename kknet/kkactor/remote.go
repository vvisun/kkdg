package kkactor

import (
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/vvisun/kkdg/utils/kkcodec"
)

const (
	remoteFuncName           = "kkactor.remote"
	remoteBytesType          = "bytes"
	remoteRawType            = "raw"
	defaultRemoteWaitTimeout = 30 * time.Second
)

// RemoteCodec encodes and decodes remote envelopes and messages.
type RemoteCodec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// Remote defines a transport for distributed actors.
type Remote interface {
	NodeID() string
	Publish(nodeID string, data []byte) error
	Request(nodeID string, data []byte, timeout ...time.Duration) ([]byte, error)
	SetPublishHandler(handler func(sourceNodeID string, data []byte))
	SetRequestHandler(handler func(sourceNodeID string, data []byte) ([]byte, error))
}

// RawMessage is delivered when message type is unknown or raw bytes are desired.
type RawMessage struct {
	Type    string
	Payload []byte
}

type remoteEnvelope struct {
	TargetID      string `json:"targetID"`
	SenderID      string `json:"senderID,omitempty"`
	MessageType   string `json:"messageType,omitempty"`
	Payload       []byte `json:"payload,omitempty"`
	TimeoutMillis int64  `json:"timeoutMillis,omitempty"`
}

type remoteResponse struct {
	MessageType string `json:"messageType,omitempty"`
	Payload     []byte `json:"payload,omitempty"`
	Error       string `json:"error,omitempty"`
}

var (
	registryByName sync.Map // string -> func() any
	registryByType sync.Map // reflect.Type -> string
)

// RegisterMessage registers a message factory by name.
func RegisterMessage(name string, factory func() any) {
	if name == "" || factory == nil {
		return
	}
	registryByName.Store(name, factory)
}

// RegisterMessageType registers a message type by name.
func RegisterMessageType(name string, sample any) {
	if name == "" || sample == nil {
		return
	}
	t := reflect.TypeOf(sample)
	factory := func() any {
		if t.Kind() == reflect.Pointer {
			return reflect.New(t.Elem()).Interface()
		}
		return reflect.New(t).Interface()
	}
	RegisterMessage(name, factory)
	registryByType.Store(t, name)
	if t.Kind() == reflect.Pointer {
		registryByType.Store(t.Elem(), name)
	}
}

func defaultRemoteCodec() RemoteCodec {
	return kkcodec.GetCodec(kkcodec.CodecTypeJson)
}

// NewActorSystemWithRemote creates a system with remote transport enabled.
func NewActorSystemWithRemote(nodeID string, remote Remote, codec RemoteCodec) *ActorSystem {
	sys := NewActorSystem()
	sys.EnableRemote(nodeID, remote, codec)
	return sys
}

// EnableRemote configures the actor system for distributed messaging.
func (s *ActorSystem) EnableRemote(nodeID string, remote Remote, codec RemoteCodec) {
	if s == nil {
		return
	}
	if remote != nil && nodeID == "" {
		nodeID = remote.NodeID()
	}
	s.nodeID = nodeID
	s.remote = remote
	if codec == nil {
		codec = defaultRemoteCodec()
	}
	s.codec = codec
	if remote == nil {
		return
	}
	remote.SetPublishHandler(func(sourceNodeID string, data []byte) {
		s.handleRemotePublish(sourceNodeID, data)
	})
	remote.SetRequestHandler(func(sourceNodeID string, data []byte) ([]byte, error) {
		return s.handleRemoteRequest(sourceNodeID, data)
	})
}

// NodeID returns the local node id.
func (s *ActorSystem) NodeID() string {
	if s == nil {
		return ""
	}
	return s.nodeID
}

// Remote returns the configured remote transport.
func (s *ActorSystem) Remote() Remote {
	if s == nil {
		return nil
	}
	return s.remote
}

func (s *ActorSystem) isRemote(pid *PID) bool {
	if s == nil || pid == nil {
		return false
	}
	if pid.system != nil && pid.system != s {
		return true
	}
	if pid.nodeID == "" {
		return false
	}
	if s.nodeID == "" {
		return true
	}
	return pid.nodeID != s.nodeID
}

func (s *ActorSystem) sendRemote(pid *PID, env *Envelope) error {
	if s == nil || pid == nil {
		return ErrActorDead
	}
	if s.remote == nil {
		return ErrRemoteNotConfigured
	}
	if pid.nodeID == "" {
		return ErrRemoteUnsupportedType
	}
	data, err := s.encodeRemoteEnvelope(pid, env)
	if err != nil {
		return err
	}
	if env.respond != nil {
		go s.doRemoteRequest(pid.nodeID, data, env.respond, env.timeout)
		return nil
	}
	go func() {
		_ = s.remote.Publish(pid.nodeID, data)
	}()
	return nil
}

func (s *ActorSystem) doRemoteRequest(nodeID string, data []byte, respond chan *FutureResult, timeout time.Duration) {
	if respond == nil {
		return
	}
	var (
		respData []byte
		err      error
	)
	if timeout > 0 {
		respData, err = s.remote.Request(nodeID, data, timeout)
	} else {
		respData, err = s.remote.Request(nodeID, data)
	}
	if err != nil {
		respond <- &FutureResult{Error: err}
		return
	}
	res, err := s.decodeRemoteResponse(respData)
	if err != nil {
		respond <- &FutureResult{Error: err}
		return
	}
	respond <- res
}

func (s *ActorSystem) handleRemotePublish(sourceNodeID string, data []byte) {
	env, err := s.decodeRemoteEnvelope(data)
	if err != nil || env.TargetID == "" {
		return
	}
	msg, err := decodeMessage(s.codec, env.MessageType, env.Payload)
	if err != nil {
		msg = RawMessage{Type: env.MessageType, Payload: env.Payload}
	}
	target := &PID{id: env.TargetID, system: s, nodeID: s.nodeID}
	sender := &PID{id: env.SenderID, nodeID: sourceNodeID}
	_ = s.sendLocal(target, &Envelope{message: msg, sender: sender})
}

func (s *ActorSystem) handleRemoteRequest(sourceNodeID string, data []byte) ([]byte, error) {
	env, err := s.decodeRemoteEnvelope(data)
	if err != nil || env.TargetID == "" {
		return s.encodeRemoteError(ErrRemoteDecodeFailed)
	}
	msg, err := decodeMessage(s.codec, env.MessageType, env.Payload)
	if err != nil {
		msg = RawMessage{Type: env.MessageType, Payload: env.Payload}
	}
	target := &PID{id: env.TargetID, system: s, nodeID: s.nodeID}
	sender := &PID{id: env.SenderID, nodeID: sourceNodeID}
	respond := make(chan *FutureResult, 1)
	if err := s.sendLocal(target, &Envelope{message: msg, sender: sender, respond: respond}); err != nil {
		return s.encodeRemoteError(err)
	}
	wait := time.Duration(env.TimeoutMillis) * time.Millisecond
	if wait <= 0 {
		wait = defaultRemoteWaitTimeout
	}
	select {
	case res := <-respond:
		return s.encodeRemoteResponse(res)
	case <-time.After(wait):
		return s.encodeRemoteError(ErrTimeout)
	}
}

func (s *ActorSystem) encodeRemoteEnvelope(pid *PID, env *Envelope) ([]byte, error) {
	if s.codec == nil {
		return nil, ErrRemoteEncodeFailed
	}
	msgType, payload, err := encodeMessage(s.codec, env.message)
	if err != nil {
		return nil, err
	}
	timeoutMillis := int64(0)
	if env.timeout > 0 {
		timeoutMillis = env.timeout.Milliseconds()
	}
	senderID := ""
	if env.sender != nil {
		senderID = env.sender.ID()
	}
	renv := &remoteEnvelope{
		TargetID:      pid.id,
		SenderID:      senderID,
		MessageType:   msgType,
		Payload:       payload,
		TimeoutMillis: timeoutMillis,
	}
	data, err := s.codec.Marshal(renv)
	if err != nil {
		return nil, ErrRemoteEncodeFailed
	}
	return data, nil
}

func (s *ActorSystem) decodeRemoteEnvelope(data []byte) (*remoteEnvelope, error) {
	if s.codec == nil {
		return nil, ErrRemoteDecodeFailed
	}
	var env remoteEnvelope
	if err := s.codec.Unmarshal(data, &env); err != nil {
		return nil, ErrRemoteDecodeFailed
	}
	return &env, nil
}

func (s *ActorSystem) encodeRemoteResponse(res *FutureResult) ([]byte, error) {
	if s.codec == nil {
		return nil, ErrRemoteEncodeFailed
	}
	resp := remoteResponse{}
	if res == nil {
		data, err := s.codec.Marshal(resp)
		if err != nil {
			return nil, ErrRemoteEncodeFailed
		}
		return data, nil
	}
	if res.Error != nil {
		resp.Error = res.Error.Error()
		data, err := s.codec.Marshal(resp)
		if err != nil {
			return nil, ErrRemoteEncodeFailed
		}
		return data, nil
	}
	if errMsg, ok := res.Message.(error); ok {
		resp.Error = errMsg.Error()
		data, err := s.codec.Marshal(resp)
		if err != nil {
			return nil, ErrRemoteEncodeFailed
		}
		return data, nil
	}
	msgType, payload, err := encodeMessage(s.codec, res.Message)
	if err != nil {
		resp.Error = err.Error()
		data, err := s.codec.Marshal(resp)
		if err != nil {
			return nil, ErrRemoteEncodeFailed
		}
		return data, nil
	}
	resp.MessageType = msgType
	resp.Payload = payload
	data, err := s.codec.Marshal(resp)
	if err != nil {
		return nil, ErrRemoteEncodeFailed
	}
	return data, nil
}

func (s *ActorSystem) encodeRemoteError(err error) ([]byte, error) {
	if s.codec == nil {
		return nil, ErrRemoteEncodeFailed
	}
	resp := remoteResponse{}
	if err != nil {
		resp.Error = err.Error()
	}
	data, encErr := s.codec.Marshal(resp)
	if encErr != nil {
		return nil, ErrRemoteEncodeFailed
	}
	return data, nil
}

func (s *ActorSystem) decodeRemoteResponse(data []byte) (*FutureResult, error) {
	if s.codec == nil {
		return nil, ErrRemoteDecodeFailed
	}
	var resp remoteResponse
	if err := s.codec.Unmarshal(data, &resp); err != nil {
		return nil, ErrRemoteDecodeFailed
	}
	if resp.Error != "" {
		return &FutureResult{Error: fmt.Errorf("%w: %s", ErrRemoteRequestFailed, resp.Error)}, nil
	}
	msg, err := decodeMessage(s.codec, resp.MessageType, resp.Payload)
	if err != nil {
		return &FutureResult{Error: ErrRemoteDecodeFailed}, nil
	}
	return &FutureResult{Message: msg}, nil
}

func encodeMessage(codec RemoteCodec, msg any) (string, []byte, error) {
	switch m := msg.(type) {
	case RawMessage:
		msgType := m.Type
		if msgType == "" {
			msgType = remoteRawType
		}
		return msgType, m.Payload, nil
	case *RawMessage:
		if m == nil {
			return "", nil, nil
		}
		msgType := m.Type
		if msgType == "" {
			msgType = remoteRawType
		}
		return msgType, m.Payload, nil
	case []byte:
		return remoteBytesType, m, nil
	default:
		msgType, ok := messageTypeName(msg)
		if !ok {
			return "", nil, ErrMessageNotRegistered
		}
		if codec == nil {
			return "", nil, ErrRemoteEncodeFailed
		}
		payload, err := codec.Marshal(msg)
		if err != nil {
			return "", nil, ErrRemoteEncodeFailed
		}
		return msgType, payload, nil
	}
}

func decodeMessage(codec RemoteCodec, msgType string, payload []byte) (any, error) {
	if msgType == "" {
		return nil, nil
	}
	switch msgType {
	case remoteBytesType:
		return payload, nil
	case remoteRawType:
		return RawMessage{Type: msgType, Payload: payload}, nil
	default:
		v, ok := newMessageByName(msgType)
		if !ok {
			return RawMessage{Type: msgType, Payload: payload}, nil
		}
		if codec == nil {
			return nil, ErrRemoteDecodeFailed
		}
		if err := codec.Unmarshal(payload, v); err != nil {
			return nil, ErrRemoteDecodeFailed
		}
		return v, nil
	}
}

func messageTypeName(msg any) (string, bool) {
	if msg == nil {
		return "", true
	}
	t := reflect.TypeOf(msg)
	if name, ok := registryByType.Load(t); ok {
		return name.(string), true
	}
	if t.Kind() == reflect.Pointer {
		if name, ok := registryByType.Load(t.Elem()); ok {
			return name.(string), true
		}
	}
	return "", false
}

func newMessageByName(name string) (any, bool) {
	if name == "" {
		return nil, false
	}
	if factory, ok := registryByName.Load(name); ok {
		return factory.(func() any)(), true
	}
	return nil, false
}
