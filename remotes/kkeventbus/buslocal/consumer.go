package buslocal

import (
	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/remotes/kkeventbus/internal"
)

type consumer struct {
	listenerMgr *internal.ListenerManager
}

func NewConsumer() *consumer {
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
func (c *consumer) dispatch(event *kkeventbus.Event) {
	c.listenerMgr.Publish(event)
}
