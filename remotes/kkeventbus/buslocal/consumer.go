package buslocal

import (
	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/remotes/kkeventbus/internal"
	"github.com/vvisun/kkdg/utils/kklog"
)

type consumer struct {
	listenerMgr *internal.ListenerManager
	registry    *kkeventbus.MessageRegistry
}

func newConsumer(registry *kkeventbus.MessageRegistry) *consumer {
	return &consumer{
		listenerMgr: internal.NewListenerManager(),
		registry:    registry,
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
	event, err := internal.Deserialize(c.registry, data)
	if err != nil {
		kklog.Errorf("invalid event data: %v", err)
		return
	}

	c.listenerMgr.Publish(event)
}
