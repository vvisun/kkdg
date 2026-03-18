package kkevent

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/xcall"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type EventListener func(data any)

type ListenerInfo struct {
	gid      uint64
	callback EventListener
}

type EventManager[K comparable] struct {
	listeners map[K][]ListenerInfo
	mu        sync.RWMutex
	autoID    atomic.Uint64
}

func NewEventManager[K comparable]() *EventManager[K] {
	return &EventManager[K]{
		listeners: make(map[K][]ListenerInfo),
	}
}

func (m *EventManager[K]) getHandler(key K, listener EventListener) (*ListenerInfo, int) {
	for i, l := range m.listeners[key] {
		if xreflect.IsSameFunc(l.callback, listener) {
			return &l, i
		}
	}
	return nil, -1
}

func (m *EventManager[K]) Subscribe(key K, listener EventListener) uint64 {
	if listener == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if info, idx := m.getHandler(key, listener); info != nil && idx != -1 {
		return info.gid
	}

	// COW: 拷贝并追加，然后替换原 map 中的切片
	current := m.listeners[key]
	newList := make([]ListenerInfo, len(current)+1)
	copy(newList, current)
	newList[len(current)] = ListenerInfo{
		gid:      m.autoID.Add(1),
		callback: listener,
	}
	m.listeners[key] = newList
	return newList[len(current)].gid
}

func (m *EventManager[K]) Unsubscribe(key K, listener EventListener) {
	if listener == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	_, idx := m.getHandler(key, listener)
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

func (m *EventManager[K]) UnsubscribeByID(key K, id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i, l := range m.listeners[key] {
		if l.gid == id {
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

func (m *EventManager[K]) UnsubscribeAll(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.listeners, key)
}

func (m *EventManager[K]) Publish(key K, data any) {
	m.mu.RLock()
	listeners := m.listeners[key]
	m.mu.RUnlock()

	for _, listener := range listeners {
		xcall.SafeCall(func() {
			listener.callback(data)
		})
	}
}
