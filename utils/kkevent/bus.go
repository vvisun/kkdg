package kkevent

import (
	"fmt"
	"reflect"
	"sync"
)

// BusSubscriber defines subscription-related bus behavior
type BusSubscriber interface {
	Subscribe(topic string, fn interface{}) error
	SubscribeAsync(topic string, fn interface{}, transactional bool) error
	SubscribeOnce(topic string, fn interface{}) error
	SubscribeOnceAsync(topic string, fn interface{}) error
	Unsubscribe(topic string, handler interface{}) error
}

// BusPublisher defines publishing-related bus behavior
type BusPublisher interface {
	Publish(topic string, args ...interface{})
}

// BusController defines bus control behavior (checking handler's presence, synchronization)
type BusController interface {
	HasCallback(topic string) bool
	WaitAsync()
}

// Bus englobes global (subscribe, publish, control) bus behavior
type Bus interface {
	BusController
	BusSubscriber
	BusPublisher
}

// EventBus - box for handlers and callbacks.
type EventBus struct {
	handlers map[string][]*eventHandler
	lock     sync.RWMutex // 改为读写锁
	wg       sync.WaitGroup
}

type eventHandler struct {
	callBack      reflect.Value
	flagOnce      bool
	async         bool
	transactional bool
	mu            sync.Mutex // 内部锁重命名，避免直接嵌入 sync.Mutex 的 Lock 方法暴露
}

// NewEventBus returns new EventBus with empty handlers.
func NewEventBus() Bus {
	return &EventBus{
		handlers: make(map[string][]*eventHandler),
	}
}

// doSubscribe handles the subscription logic
func (bus *EventBus) doSubscribe(topic string, fn interface{}, handler *eventHandler) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	v := reflect.ValueOf(fn)
	if v.Kind() != reflect.Func {
		return fmt.Errorf("%s is not of type reflect.Func", v.Kind())
	}

	// COW: 拷贝并追加，然后替换原 map 中的切片
	current := bus.handlers[topic]
	newList := make([]*eventHandler, len(current)+1)
	copy(newList, current)
	newList[len(current)] = handler
	bus.handlers[topic] = newList

	return nil
}

// Subscribe subscribes to a topic.
func (bus *EventBus) Subscribe(topic string, fn interface{}) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack: reflect.ValueOf(fn),
	})
}

// SubscribeAsync subscribes to a topic with an asynchronous callback
func (bus *EventBus) SubscribeAsync(topic string, fn interface{}, transactional bool) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack:      reflect.ValueOf(fn),
		async:         true,
		transactional: transactional,
	})
}

// SubscribeOnce subscribes to a topic once.
func (bus *EventBus) SubscribeOnce(topic string, fn interface{}) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack: reflect.ValueOf(fn),
		flagOnce: true,
	})
}

// SubscribeOnceAsync subscribes to a topic once with an asynchronous callback
func (bus *EventBus) SubscribeOnceAsync(topic string, fn interface{}) error {
	return bus.doSubscribe(topic, fn, &eventHandler{
		callBack: reflect.ValueOf(fn),
		flagOnce: true,
		async:    true,
	})
}

// HasCallback returns true if exists any callback subscribed to the topic.
func (bus *EventBus) HasCallback(topic string) bool {
	bus.lock.RLock()
	defer bus.lock.RUnlock()
	handlers, ok := bus.handlers[topic]
	return ok && len(handlers) > 0
}

// Unsubscribe removes callback defined for a topic.
func (bus *EventBus) Unsubscribe(topic string, handler interface{}) error {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	handlers, ok := bus.handlers[topic]
	if !ok || len(handlers) == 0 {
		return fmt.Errorf("topic %s doesn't exist", topic)
	}

	v := reflect.ValueOf(handler)
	idx := -1
	for i, h := range handlers {
		if h.callBack.Type() == v.Type() && h.callBack.Pointer() == v.Pointer() {
			idx = i
			break
		}
	}

	if idx == -1 {
		return fmt.Errorf("handler not found for topic %s", topic)
	}

	// COW: 创建新切片
	newList := make([]*eventHandler, len(handlers)-1)
	copy(newList, handlers[:idx])
	copy(newList[idx:], handlers[idx+1:])
	if len(newList) > 0 {
		bus.handlers[topic] = newList
	} else {
		delete(bus.handlers, topic)
	}

	return nil
}

// Publish executes callback defined for a topic.
func (bus *EventBus) Publish(topic string, args ...interface{}) {
	bus.lock.RLock()
	handlers, ok := bus.handlers[topic]
	if !ok || len(handlers) == 0 {
		bus.lock.RUnlock()
		return
	}
	// 在 RLock 保护下持有当前监听器列表引用
	// 因为是 COW，订阅/取消订阅会替换 map 中的 slice 指针，而我们持有的引用是稳定的
	currentHandlers := handlers
	bus.lock.RUnlock()

	var hasOnce bool
	var passedArgs []reflect.Value

	for _, handler := range currentHandlers {
		if handler.flagOnce {
			hasOnce = true
		}

		// 延迟初始化参数（只在第一次有监听器时构造）
		if passedArgs == nil {
			passedArgs = bus.setUpPublish(handler, args...)
		}

		if !handler.async {
			handler.callBack.Call(passedArgs)
		} else {
			bus.wg.Add(1)
			go func(h *eventHandler, a []reflect.Value) {
				defer bus.wg.Done()
				if h.transactional {
					h.mu.Lock()
					defer h.mu.Unlock()
				}
				h.callBack.Call(a)
			}(handler, passedArgs)
		}
	}

	// 处理 Once 逻辑：如果存在 Once 监听器，执行一次清理
	if hasOnce {
		bus.removeOnceHandlers(topic, currentHandlers)
	}
}

func (bus *EventBus) removeOnceHandlers(topic string, executed []*eventHandler) {
	bus.lock.Lock()
	defer bus.lock.Unlock()

	current, ok := bus.handlers[topic]
	if !ok {
		return
	}

	// 过滤掉已经执行过的 Once 监听器
	newList := make([]*eventHandler, 0, len(current))
	for _, h := range current {
		isExecutedOnce := false
		if h.flagOnce {
			for _, e := range executed {
				if e == h {
					isExecutedOnce = true
					break
				}
			}
		}
		if !isExecutedOnce {
			newList = append(newList, h)
		}
	}

	if len(newList) != len(current) {
		if len(newList) > 0 {
			bus.handlers[topic] = newList
		} else {
			delete(bus.handlers, topic)
		}
	}
}

func (bus *EventBus) setUpPublish(callback *eventHandler, args ...interface{}) []reflect.Value {
	funcType := callback.callBack.Type()
	numArgs := funcType.NumIn()
	passedArguments := make([]reflect.Value, numArgs)

	for i := 0; i < numArgs; i++ {
		if i < len(args) && args[i] != nil {
			argVal := reflect.ValueOf(args[i])
			// 简单的类型检查增强健壮性
			if argVal.Type().AssignableTo(funcType.In(i)) {
				passedArguments[i] = argVal
			} else {
				passedArguments[i] = reflect.New(funcType.In(i)).Elem()
			}
		} else {
			passedArguments[i] = reflect.New(funcType.In(i)).Elem()
		}
	}

	return passedArguments
}

// WaitAsync waits for all async callbacks to complete
func (bus *EventBus) WaitAsync() {
	bus.wg.Wait()
}
