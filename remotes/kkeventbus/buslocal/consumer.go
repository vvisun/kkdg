package buslocal

import (
	"reflect"
	"sync"

	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/utils/xcall"
)

type consumer struct {
	rw       sync.RWMutex
	handlers map[uintptr][]kkeventbus.EventHandler
}

func NewConsumer() *consumer {
	return &consumer{
		handlers: make(map[uintptr][]kkeventbus.EventHandler),
	}
}

// 添加处理器
func (c *consumer) addHandler(handler kkeventbus.EventHandler) int {
	pointer := reflect.ValueOf(handler).Pointer()

	c.rw.Lock()
	defer c.rw.Unlock()

	if _, ok := c.handlers[pointer]; !ok {
		c.handlers[pointer] = make([]kkeventbus.EventHandler, 0, 1)
	}

	c.handlers[pointer] = append(c.handlers[pointer], handler)

	return len(c.handlers[pointer])
}

// 移除处理器
func (c *consumer) delHandler(handler kkeventbus.EventHandler) int {
	pointer := reflect.ValueOf(handler).Pointer()

	c.rw.Lock()
	defer c.rw.Unlock()

	delete(c.handlers, pointer)

	return len(c.handlers)
}

// 分发数据
func (c *consumer) dispatch(event *kkeventbus.Event) {
	c.rw.RLock()
	defer c.rw.RUnlock()

	for _, handlers := range c.handlers {
		for i := range handlers {
			handler := handlers[i]
			xcall.AntsSafeGo(func() { handler(event) })
		}
	}
}
