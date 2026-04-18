package busnats

import (
	"context"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/remotes/kkeventbus/internal"
	"github.com/vvisun/kkdg/utils/kklog"
)

type Eventbus struct {
	err       error
	builtin   bool
	opts      Options
	registry  *kkeventbus.MessageRegistry
	rw        sync.RWMutex
	consumers map[string]*consumer
}

var _ kkeventbus.IEventBus = (*Eventbus)(nil)

func NewEventbus(registry *kkeventbus.MessageRegistry, opts Options) (*Eventbus, error) {
	if registry == nil {
		return nil, kkeventbus.ErrRegistryNil
	}

	eb := &Eventbus{opts: opts, registry: registry}
	eb.consumers = make(map[string]*consumer)

	if opts.conn == nil {
		opts.conn, eb.err = nats.Connect(opts.Url, nats.Timeout(opts.Timeout))
		eb.builtin = true
	}

	return eb, eb.err
}

// Publish 发布事件
func (eb *Eventbus) Publish(_ context.Context, topic string, payload any) error {
	if eb.err != nil {
		return eb.err
	}

	buf, err := internal.Serialize(eb.registry, topic, payload)
	if err != nil {
		return err
	}

	return eb.opts.conn.Publish(eb.doMakeChannel(topic), buf)
}

// Subscribe 订阅事件
func (eb *Eventbus) Subscribe(_ context.Context, topic string, handler kkeventbus.EventHandler) (uint64, error) {
	if eb.err != nil {
		return 0, eb.err
	}

	if handler == nil {
		return 0, kkerrors.ErrInvalidHandler
	}

	channel := eb.doMakeChannel(topic)

	eb.rw.Lock()
	defer eb.rw.Unlock()

	c, ok := eb.consumers[channel]
	if !ok {
		c = newConsumer(eb.registry)
		sub, err := eb.opts.conn.Subscribe(channel, func(msg *nats.Msg) {
			c.dispatch(msg.Data)
		})
		if err != nil {
			return 0, err
		}
		c.sub = sub
		eb.consumers[channel] = c
	}

	return c.addHandler(handler), nil
}

// Unsubscribe 取消订阅
func (eb *Eventbus) Unsubscribe(_ context.Context, topic string, handler kkeventbus.EventHandler) error {
	if eb.err != nil {
		return eb.err
	}

	if handler == nil {
		return kkerrors.ErrInvalidHandler
	}

	channel := eb.doMakeChannel(topic)

	eb.rw.Lock()
	defer eb.rw.Unlock()

	if c, ok := eb.consumers[channel]; ok {
		if c.delHandler(handler) != 0 {
			return nil
		}

		if err := c.sub.Unsubscribe(); err != nil {
			return err
		}

		delete(eb.consumers, channel)
	}

	return nil
}

// UnsubscribeAll 取消所有订阅
func (eb *Eventbus) UnsubscribeAll(_ context.Context) error {
	if eb.err != nil {
		return eb.err
	}

	eb.rw.Lock()
	defer eb.rw.Unlock()

	for _, c := range eb.consumers {
		c.listenerMgr.UnsubscribeAll()

		if err := c.sub.Unsubscribe(); err != nil {
			kklog.Errorf("unsubscribe failed: %v", err)
		}
	}

	eb.consumers = make(map[string]*consumer)

	return nil
}

// UnsubscribeByID 根据ID取消订阅
func (eb *Eventbus) UnsubscribeByID(_ context.Context, topic string, id uint64) error {
	if eb.err != nil {
		return eb.err
	}

	channel := eb.doMakeChannel(topic)

	eb.rw.Lock()
	defer eb.rw.Unlock()

	if c, ok := eb.consumers[channel]; ok {
		c.listenerMgr.UnsubscribeByID(id)
		if c.listenerMgr.GetListenerCount() != 0 {
			return nil
		}

		if err := c.sub.Unsubscribe(); err != nil {
			return err
		}

		delete(eb.consumers, channel)
	}

	return nil
}

// Close 停止监听
//
//	如果conn是外部连接，则不关闭，由外部管理。
func (eb *Eventbus) Close() error {
	if eb.err != nil {
		return eb.err
	}

	eb.UnsubscribeAll(context.Background())

	if eb.builtin {
		eb.opts.conn.Close()
	}

	return nil
}

func (eb *Eventbus) doMakeChannel(topic string) string {
	if eb.opts.TopicPrefix == "" {
		return topic
	} else {
		return eb.opts.TopicPrefix + ":" + topic
	}
}
