package busnats

import (
	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/remotes/kkeventbus/internal"
	"github.com/vvisun/kkdg/utils/kklog"
)

type consumer struct {
	sub         *nats.Subscription
	listenerMgr *internal.ListenerManager
}

func newConsumer() *consumer {
	return &consumer{
		listenerMgr: internal.NewListenerManager(),
	}
}

// 添加处理器
func (c *consumer) addHandler(handler kkeventbus.EventHandler) uint64 {
	return c.listenerMgr.Subscribe(handler)
}

// 移除处理器
func (c *consumer) delHandler(handler kkeventbus.EventHandler) int {
	c.listenerMgr.Unsubscribe(handler)
	return c.listenerMgr.GetListenerCount()
}

// 分发数据
func (c *consumer) dispatch(data []byte) {
	event, err := deserialize(data)
	if err != nil {
		kklog.Errorf("invalid event data: %v", err)
		return
	}

	c.listenerMgr.Publish(event)
}
