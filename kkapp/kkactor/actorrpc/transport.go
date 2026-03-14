package actorrpc

import (
	"errors"
	"sync"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// actorrpc transport not implemented. 未来有需求再实现。
var errActorRpcTransportNotImplemented = errors.New("actor rpc transport not implemented")

type Transport struct {
	nodeID   string
	registry *actorremotes.MessageRegistry
	receiver actorremotes.IRemoteActorReceiver
	mu       sync.RWMutex
}

var _ actorremotes.IRemoteActorTransport = (*Transport)(nil)

func NewTransport(nodeID string, registry *actorremotes.MessageRegistry) *Transport {
	if registry == nil {
		kklog.PanicLog("MessageRegistry is nil")
		return nil
	}
	return &Transport{
		nodeID:   nodeID,
		registry: registry,
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
	return errActorRpcTransportNotImplemented
}

func (t *Transport) Close() error {
	return nil
}

func (t *Transport) Send(target actorremotes.ActorRef, msg any) error {
	return errActorRpcTransportNotImplemented
}

func (t *Transport) Request(target actorremotes.ActorRef, msg any, timeout time.Duration) (any, error) {
	return nil, errActorRpcTransportNotImplemented
}

func (t *Transport) RequestAsync(target actorremotes.ActorRef, msg any, timeout time.Duration, callback func(result any, err error)) error {
	if callback == nil {
		return kkerrors.ErrActorAsyncCallbackNil
	}
	callback(nil, errActorRpcTransportNotImplemented)
	return errActorRpcTransportNotImplemented
}
