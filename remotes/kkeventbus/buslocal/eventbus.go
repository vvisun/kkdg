package buslocal

import (
	"context"
	"sync"

	"github.com/vvisun/kkdg/remotes/kkeventbus"
	xvalue "github.com/vvisun/kkdg/utils/value"
	"github.com/vvisun/kkdg/utils/xtime"
	"github.com/vvisun/kkdg/utils/xuuid"
)

type Eventbus struct {
	rw        sync.RWMutex
	consumers map[string]*consumer
}

var _ kkeventbus.IEventBus = (*Eventbus)(nil)

func NewEventbus() *Eventbus {
	eb := &Eventbus{}
	eb.consumers = make(map[string]*consumer)

	return eb
}

// Publish 发布事件
func (eb *Eventbus) Publish(_ context.Context, topic string, payload any) error {
	eb.rw.RLock()
	defer eb.rw.RUnlock()

	c, ok := eb.consumers[topic]
	if !ok {
		return nil
	}

	c.dispatch(&kkeventbus.Event{
		ID:        xuuid.UUID(),
		Topic:     topic,
		Payload:   xvalue.NewValue(payload),
		Timestamp: xtime.UnixNano(xtime.Now().UnixNano()),
	})

	return nil
}

// Subscribe 订阅事件
func (eb *Eventbus) Subscribe(_ context.Context, topic string, handler kkeventbus.EventHandler) error {
	eb.rw.Lock()
	defer eb.rw.Unlock()

	c, ok := eb.consumers[topic]
	if !ok {
		c = &consumer{handlers: make(map[uintptr][]kkeventbus.EventHandler, 1)}
		eb.consumers[topic] = c
	}

	c.addHandler(handler)

	return nil
}

// Unsubscribe 取消订阅
func (eb *Eventbus) Unsubscribe(_ context.Context, topic string, handler kkeventbus.EventHandler) error {
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

// Close 停止监听
func (eb *Eventbus) Close() error {
	return nil
}
