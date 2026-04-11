package internal

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/remotes/kkeventbus"
	"github.com/vvisun/kkdg/utils/xcall"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type ListenerInfo struct {
	gid      uint64
	callback kkeventbus.EventHandler
}

type ListenerManager struct {
	listeners []ListenerInfo
	mu        sync.RWMutex
	autoID    atomic.Uint64
}

func NewListenerManager() *ListenerManager {
	return &ListenerManager{
		listeners: make([]ListenerInfo, 0),
	}
}

func (m *ListenerManager) getHandlerIdx(listener kkeventbus.EventHandler) (uint64, int) {
	for i, l := range m.listeners {
		if xreflect.IsSameFunc(l.callback, listener) {
			return l.gid, i
		}
	}
	return 0, -1
}

func (m *ListenerManager) Subscribe(listener kkeventbus.EventHandler) uint64 {
	if listener == nil {
		return 0
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	if gid, idx := m.getHandlerIdx(listener); idx != -1 {
		return gid
	}

	// COW: 拷贝并追加，再原子替换 listeners 切片
	current := m.listeners
	newList := make([]ListenerInfo, len(current)+1)
	copy(newList, current)
	newList[len(current)] = ListenerInfo{
		gid:      m.autoID.Add(1),
		callback: listener,
	}
	m.listeners = newList
	return newList[len(current)].gid
}

func (m *ListenerManager) Unsubscribe(listener kkeventbus.EventHandler) {
	if listener == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	_, idx := m.getHandlerIdx(listener)
	if idx == -1 {
		return
	}

	// COW: 拷贝并删除，再原子替换 listeners 切片
	newLen := len(m.listeners) - 1
	if newLen > 0 {
		newList := make([]ListenerInfo, newLen)
		copy(newList, m.listeners[:idx])
		copy(newList[idx:], m.listeners[idx+1:])
		m.listeners = newList
	} else {
		m.listeners = make([]ListenerInfo, 0)
	}
}

func (m *ListenerManager) UnsubscribeByID(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	idx := -1
	for i, l := range m.listeners {
		if l.gid == id {
			idx = i
			break
		}
	}
	if idx == -1 {
		return
	}

	// COW: 拷贝并删除，再原子替换 listeners 切片
	newLen := len(m.listeners) - 1
	if newLen > 0 {
		newList := make([]ListenerInfo, newLen)
		copy(newList, m.listeners[:idx])
		copy(newList[idx:], m.listeners[idx+1:])
		m.listeners = newList
	} else {
		m.listeners = make([]ListenerInfo, 0)
	}
}

func (m *ListenerManager) UnsubscribeAll() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.listeners = make([]ListenerInfo, 0)
}

func (m *ListenerManager) Publish(event *kkeventbus.Event) {
	m.mu.RLock()
	listeners := m.listeners
	m.mu.RUnlock()

	for _, listener := range listeners {
		xcall.AntsSafeGo(func() {
			listener.callback(event)
		})
	}
}

func (m *ListenerManager) GetListenerCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.listeners)
}
