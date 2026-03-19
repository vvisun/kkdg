package kkevent

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/xcall"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type SpecEventListener[T any] func(data T)

type SpecListenerInfo[T any] struct {
	gid      uint64
	callback SpecEventListener[T]
}

type SpecEventManager[K comparable, T any] struct {
	listeners map[K][]SpecListenerInfo[T]
	mu        sync.RWMutex
	autoID    atomic.Uint64
}

func NewSpecEventManager[K comparable, T any]() *SpecEventManager[K, T] {
	return &SpecEventManager[K, T]{
		listeners: make(map[K][]SpecListenerInfo[T]),
	}
}

func (m *SpecEventManager[K, T]) getHandlerIdx(key K, listener SpecEventListener[T]) (uint64, int) {
	for i, l := range m.listeners[key] {
		if xreflect.IsSameFunc(l.callback, listener) {
			return l.gid, i
		}
	}
	return 0, -1
}

func (m *SpecEventManager[K, T]) Subscribe(key K, listener SpecEventListener[T]) uint64 {
	if listener == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if gid, idx := m.getHandlerIdx(key, listener); idx != -1 {
		return gid
	}

	// COW: 拷贝并追加，然后替换原 map 中的切片
	current := m.listeners[key]
	newList := make([]SpecListenerInfo[T], len(current)+1)
	copy(newList, current)
	newList[len(current)] = SpecListenerInfo[T]{
		gid:      m.autoID.Add(1),
		callback: listener,
	}
	m.listeners[key] = newList
	return newList[len(current)].gid
}

func (m *SpecEventManager[K, T]) Unsubscribe(key K, listener SpecEventListener[T]) {
	if listener == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	_, idx := m.getHandlerIdx(key, listener)
	if idx == -1 {
		return
	}

	// COW: 拷贝并删除，然后替换原 map 中的切片
	current := m.listeners[key]
	newLen := len(current) - 1
	if newLen > 0 {
		newList := make([]SpecListenerInfo[T], newLen)
		copy(newList, current[:idx])
		copy(newList[idx:], current[idx+1:])
		m.listeners[key] = newList
	} else {
		delete(m.listeners, key)
	}
}

func (m *SpecEventManager[K, T]) UnsubscribeByID(key K, id uint64) {
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
	current := m.listeners[key]
	newLen := len(current) - 1
	if newLen > 0 {
		newList := make([]SpecListenerInfo[T], newLen)
		copy(newList, current[:idx])
		copy(newList[idx:], current[idx+1:])
		m.listeners[key] = newList
	} else {
		delete(m.listeners, key)
	}
}

func (m *SpecEventManager[K, T]) UnsubscribeAll(key K) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.listeners, key)
}

// Publish 安全地发布事件，进行安全调用，进行错误处理
//
//	主要用于非核心事件处理，一旦处理失败应该吞没panic，防止打蹦进程
func (m *SpecEventManager[K, T]) Publish(key K, data T) {
	m.mu.RLock()
	listeners := m.listeners[key]
	m.mu.RUnlock()

	for _, listener := range listeners {
		xcall.SafeCall(func() {
			listener.callback(data)
		})
	}
}

// UnsafePublish 不安全地发布事件，不进行安全调用，不进行错误处理
//
//	主要用于核心事件处理，一旦处理失败不应该吞没panic，而是应该让他panic，防止将问题隐藏，导致不可预测的错误
func (m *SpecEventManager[K, T]) UnsafePublish(key K, data T) {
	m.mu.RLock()
	listeners := m.listeners[key]
	m.mu.RUnlock()

	for _, listener := range listeners {
		listener.callback(data)
	}
}
