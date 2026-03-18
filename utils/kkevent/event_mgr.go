package kkevent

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type ListenerInfo struct {
	ID   uint64
	Func EventListener
}

type EventListener func(data any)

type EventManager[K comparable, T any] struct {
	listeners map[K][]ListenerInfo
	mu        sync.RWMutex
	autoID    atomic.Uint64
}

func NewEventManager[K comparable, T any]() *EventManager[K, T] {
	return &EventManager[K, T]{
		listeners: make(map[K][]ListenerInfo),
	}
}

func (m *EventManager[K, T]) AddListener(key K, listener EventListener) uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, l := range m.listeners[key] {
		if xreflect.IsSameFunc(l.Func, listener) {
			return l.ID
		}
	}

	// COW: 拷贝并追加，然后替换原 map 中的切片
	current := m.listeners[key]
	newList := make([]ListenerInfo, len(current)+1)
	copy(newList, current)
	newList[len(current)] = ListenerInfo{
		ID:   m.autoID.Add(1),
		Func: listener,
	}
	m.listeners[key] = newList
	return newList[len(current)].ID
}

func (m *EventManager[K, T]) RemoveListener(key K, listener EventListener) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i, l := range m.listeners[key] {
		if xreflect.IsSameFunc(l, listener) {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}

	// COW: 拷贝并删除，然后替换原 map 中的切片
	newLen := len(m.listeners[key]) - 1
	if newLen > 0 {
		newList := make([]ListenerInfo, newLen)
		copy(newList, m.listeners[key][:idx])
		copy(newList[idx:], m.listeners[key][idx+1:])
		m.listeners[key] = newList
	} else {
		delete(m.listeners, key)
	}
}

func (m *EventManager[K, T]) RemoveListenerByID(key K, id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i, l := range m.listeners[key] {
		if l.ID == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}

	// COW: 拷贝并删除，然后替换原 map 中的切片
	newLen := len(m.listeners[key]) - 1
	if newLen > 0 {
		newList := make([]ListenerInfo, newLen)
		copy(newList, m.listeners[key][:idx])
		copy(newList[idx:], m.listeners[key][idx+1:])
		m.listeners[key] = newList
	} else {
		delete(m.listeners, key)
	}
}

func (m *EventManager[K, T]) RemoveAllListeners(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.listeners, key)
}

func (m *EventManager[K, T]) Publish(key K, data T) {
	m.mu.RLock()
	listeners := m.listeners[key]
	m.mu.RUnlock()

	for _, listener := range listeners {
		defer func() {
			if r := recover(); r != nil {
				kklog.Errorf("EventManager publish panic: %v", r)
			}
		}()
		listener.Func(data)
	}
}
