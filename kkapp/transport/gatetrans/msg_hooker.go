package gatetrans

import (
	"sync"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type MsgHookListener = func(msgId kkpacket.MSGID, data any)

type MsgHooker struct {
	muListeners sync.RWMutex
	listeners   []MsgHookListener
}

func NewMsgHooker() *MsgHooker {
	return &MsgHooker{}
}

func (h *MsgHooker) AddListener(listener MsgHookListener) {
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
	newList := make([]MsgHookListener, len(current)+1)
	copy(newList, current)
	newList[len(current)] = listener
	h.listeners = newList
}

func (h *MsgHooker) RemoveListener(listener MsgHookListener) {
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
		newList := make([]MsgHookListener, newLen)
		copy(newList, h.listeners[:idx])
		copy(newList[idx:], h.listeners[idx+1:])
		h.listeners = newList
	} else {
		h.listeners = make([]MsgHookListener, 0)
	}
}

func (h *MsgHooker) RemoveAllListeners() {
	h.muListeners.Lock()
	h.listeners = make([]MsgHookListener, 0)
	h.muListeners.Unlock()
}

func (h *MsgHooker) Notify(msgId kkpacket.MSGID, data any) {
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
			listener(msgId, data)
		}()
	}
}
