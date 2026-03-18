package kkevent

import (
	"sync"

	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type EventManager[EVT any, DATA any] struct {
	muListeners sync.RWMutex
	listeners   []func(evtId EVT, data DATA)
}

func NewEventManager[EVT any, DATA any]() *EventManager[EVT, DATA] {
	return &EventManager[EVT, DATA]{}
}

func (h *EventManager[EVT, DATA]) AddListener(listener func(evtId EVT, data DATA)) {
	if listener == nil {
		return
	}
	h.muListeners.Lock()
	defer h.muListeners.Unlock()

	for _, l := range h.listeners {
		if xreflect.IsSameFunc(l, listener) {
			return
		}
	}

	// COW: 拷贝并追加，然后替换原 map 中的切片
	current := h.listeners
	newList := make([]func(evtId EVT, data DATA), len(current)+1)
	copy(newList, current)
	newList[len(current)] = listener
	h.listeners = newList
}

func (h *EventManager[EVT, DATA]) RemoveListener(listener func(evtId EVT, data DATA)) {
	if listener == nil {
		return
	}
	h.muListeners.Lock()
	defer h.muListeners.Unlock()

	idx := -1
	for i, l := range h.listeners {
		if xreflect.IsSameFunc(l, listener) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}

	// COW: 拷贝并删除，然后替换原 map 中的切片
	newLen := len(h.listeners) - 1
	if newLen > 0 {
		newList := make([]func(evtId EVT, data DATA), newLen)
		copy(newList, h.listeners[:idx])
		copy(newList[idx:], h.listeners[idx+1:])
		h.listeners = newList
	} else {
		h.listeners = make([]func(evtId EVT, data DATA), 0)
	}
}

func (h *EventManager[EVT, DATA]) RemoveAllListeners() {
	h.muListeners.Lock()
	h.listeners = make([]func(evtId EVT, data DATA), 0)
	h.muListeners.Unlock()
}

func (h *EventManager[EVT, DATA]) Notify(evtId EVT, data DATA) {
	h.muListeners.RLock()
	if len(h.listeners) == 0 {
		h.muListeners.RUnlock()
		return
	}

	// 在 RLock 保护下持有当前监听器列表引用
	// 因为是 COW，订阅/取消订阅会替换 map 中的 slice 指针，而我们持有的引用是稳定的
	listeners := h.listeners
	h.muListeners.RUnlock()

	for _, listener := range listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					kklog.Errorf("MsgHooker notify listener panic: %v", r)
				}
			}()
			listener(evtId, data)
		}()
	}
}
