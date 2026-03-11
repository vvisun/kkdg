package actornats

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

const (
	subjectSendPrefix    = "kkactor.send."
	subjectRequestPrefix = "kkactor.request."
)

type Transport struct {
	nodeID   string
	options  nats.Options
	conn     *nats.Conn
	sendSub  *nats.Subscription
	reqSub   *nats.Subscription
	receiver actorremotes.IRemoteActorReceiver
	registry *actorremotes.MessageRegistry
	mu       sync.RWMutex
}

var _ actorremotes.IRemoteActorTransport = (*Transport)(nil)

func NewTransport(nodeID string, registry *actorremotes.MessageRegistry, options nats.Options) *Transport {
	if registry == nil {
		kklog.Errorf("[kkactor] registry is nil, use default registry")
		registry = actorremotes.GetDefaultMessageRegistry()
	}
	return &Transport{
		nodeID:   nodeID,
		registry: registry,
		options:  options,
	}
}

func (t *Transport) SetReceiver(receiver actorremotes.IRemoteActorReceiver) {
	t.mu.Lock()
	t.receiver = receiver
	t.mu.Unlock()
}

func (t *Transport) Start() error {
	if !kkapp.IsValidActorNodeId(t.nodeID) || t.nodeID == "" {
		return kkerrors.ErrActorInvalidNodeId
	}
	if t.conn != nil && t.conn.IsConnected() {
		return nil
	}

	conn, err := t.options.Connect()
	if err != nil {
		return err
	}
	t.conn = conn

	if err := t.subscribe(); err != nil {
		conn.Close()
		t.conn = nil
		return err
	}
	return nil
}

func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.sendSub != nil {
		_ = t.sendSub.Unsubscribe()
		t.sendSub = nil
	}
	if t.reqSub != nil {
		_ = t.reqSub.Unsubscribe()
		t.reqSub = nil
	}
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
	return nil
}

func (t *Transport) Send(target actorremotes.ActorRef, msg any) error {
	if !target.IsValid() {
		return kkerrors.ErrActorRemoteInvalidTarget
	}
	data, err := actorremotes.EncodeRequestEnvelope(target, msg, 0)
	if err != nil {
		return err
	}
	return t.publish(subjectSendPrefix+target.NodeID, data)
}

func (t *Transport) Request(target actorremotes.ActorRef, msg any, timeout time.Duration) (any, error) {
	if !target.IsValid() {
		return nil, kkerrors.ErrActorRemoteInvalidTarget
	}
	data, err := actorremotes.EncodeRequestEnvelope(target, msg, timeout)
	if err != nil {
		return nil, err
	}
	respMsg, err := t.request(subjectRequestPrefix+target.NodeID, data, timeout)
	if err != nil {
		return nil, err
	}
	return actorremotes.DecodeResponseEnvelope(respMsg.Data)
}

func (t *Transport) RequestAsync(target actorremotes.ActorRef, msg any, timeout time.Duration, callback func(result any, err error)) error {
	if callback == nil {
		return kkerrors.ErrActorAsyncCallbackNil
	}
	go func() {
		result, err := t.Request(target, msg, timeout)
		callback(result, err)
	}()
	return nil
}

func (t *Transport) subscribe() error {
	sendSub, err := t.conn.Subscribe(subjectSendPrefix+t.nodeID, t.handleSend)
	if err != nil {
		return err
	}
	reqSub, err := t.conn.Subscribe(subjectRequestPrefix+t.nodeID, t.handleRequest)
	if err != nil {
		_ = sendSub.Unsubscribe()
		return err
	}
	t.sendSub = sendSub
	t.reqSub = reqSub
	return t.conn.Flush()
}

func (t *Transport) handleSend(msg *nats.Msg) {
	env, payload, err := actorremotes.DecodeRequestEnvelope(msg.Data)
	if err != nil {
		kklog.Errorf("[kkactor] decode remote send failed: %v", err)
		return
	}
	receiver, err := t.getReceiver()
	if err != nil {
		kklog.Errorf("[kkactor] remote send receiver not ready: %v", err)
		return
	}
	if err := receiver.HandleRemoteSend(env.Target, payload); err != nil {
		kklog.Errorf("[kkactor] handle remote send failed: %v", err)
	}
}

func (t *Transport) handleRequest(msg *nats.Msg) {
	env, payload, err := actorremotes.DecodeRequestEnvelope(msg.Data)
	if err != nil {
		t.respond(msg, nil, err)
		return
	}
	receiver, err := t.getReceiver()
	if err != nil {
		t.respond(msg, nil, err)
		return
	}

	timeout := time.Duration(env.TimeoutMs) * time.Millisecond
	result, err := receiver.HandleRemoteRequest(env.Target, payload, timeout)
	if err != nil {
		t.respond(msg, nil, err)
		return
	}
	t.respond(msg, result, nil)
}

func (t *Transport) respond(msg *nats.Msg, result any, resultErr error) {
	data, err := actorremotes.EncodeResponseEnvelope(result, resultErr)
	if err != nil {
		kklog.Errorf("[kkactor] marshal remote response failed: %v", err)
		return
	}
	if err := msg.Respond(data); err != nil {
		kklog.Errorf("[kkactor] respond remote request failed: %v", err)
	}
}

func (t *Transport) getReceiver() (actorremotes.IRemoteActorReceiver, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.receiver == nil {
		return nil, kkerrors.ErrActorRemoteReceiverNotSet
	}
	return t.receiver, nil
}

func (t *Transport) publish(subject string, data []byte) error {
	if t.conn == nil {
		return kkerrors.ErrActorRemoteTransportNotConfigured
	}
	return t.conn.Publish(subject, data)
}

func (t *Transport) request(subject string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	if t.conn == nil {
		return nil, kkerrors.ErrActorRemoteTransportNotConfigured
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	return t.conn.Request(subject, data, timeout)
}
