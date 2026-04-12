package buslocal

import (
	"context"
	"sync"

	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/remotes/kkeventbus/internal"
)

type Eventbus struct {
	registry  *kkeventbus.MessageRegistry
	rw        sync.RWMutex
	consumers map[string]*consumer
}

var _ kkeventbus.IEventBus = (*Eventbus)(nil)

func NewEventbus(registry *kkeventbus.MessageRegistry) (*Eventbus, error) {
	if registry == nil {
		return nil, kkeventbus.ErrRegistryNil
	}
	return &Eventbus{
		registry:  registry,
		consumers: make(map[string]*consumer),
	}, nil
}

// Publish 发布事件
func (eb *Eventbus) Publish(_ context.Context, topic string, payload any) error {
	eb.rw.RLock()
	defer eb.rw.RUnlock()

	c, ok := eb.consumers[topic]
	if !ok {
		return nil
	}

	buf, err := internal.Serialize(eb.registry, topic, payload)
	if err != nil {
		return err
	}

	c.dispatch(buf)

	return nil
}

// Subscribe 订阅事件
func (eb *Eventbus) Subscribe(_ context.Context, topic string, handler kkeventbus.EventHandler) (uint64, error) {
	if handler == nil {
		return 0, kkerrors.ErrInvalidHandler
	}

	eb.rw.Lock()
	defer eb.rw.Unlock()

	c, ok := eb.consumers[topic]
	if !ok {
		c = newConsumer(eb.registry)
		eb.consumers[topic] = c
	}

	return c.addHandler(handler), nil
}

// Unsubscribe 取消订阅
func (eb *Eventbus) Unsubscribe(_ context.Context, topic string, handler kkeventbus.EventHandler) error {
	if handler == nil {
		return kkerrors.ErrInvalidHandler
	}

	eb.rw.Lock()
	defer eb.rw.Unlock()

	if c, ok := eb.consumers[topic]; ok {
		if c.delHandler(handler) != 0 {
			return nil
		}
		delete(eb.consumers, topic)
	}

	return nil
}

// UnsubscribeByID 根据ID取消订阅
func (eb *Eventbus) UnsubscribeByID(_ context.Context, topic string, id uint64) error {
	eb.rw.Lock()
	defer eb.rw.Unlock()

	if c, ok := eb.consumers[topic]; ok {
		c.listenerMgr.UnsubscribeByID(id)
		if c.listenerMgr.GetListenerCount() != 0 {
			return nil
		}
		delete(eb.consumers, topic)
	}
	return nil
}

// UnsubscribeAll 取消所有订阅
func (eb *Eventbus) UnsubscribeAll(_ context.Context) error {
	eb.rw.Lock()
	defer eb.rw.Unlock()
	eb.consumers = make(map[string]*consumer)
	return nil
}

// Close 停止监听
func (eb *Eventbus) Close() error {
	eb.rw.Lock()
	defer eb.rw.Unlock()

	for _, c := range eb.consumers {
		c.listenerMgr.UnsubscribeAll()
	}

	eb.consumers = make(map[string]*consumer)

	return nil
}
