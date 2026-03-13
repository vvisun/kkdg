package gatetrans

import (
	"sync"

	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xreflect"
)

type MsgHookListener = func(msgId kkpacket.MSGID, data any)

type MsgHooker struct {
	listeners   []MsgHookListener
	muListeners sync.RWMutex
}

func NewMsgHooker() *MsgHooker {
	return &MsgHooker{}
}

func (h *MsgHooker) AddListener(listener MsgHookListener) {
	if listener == nil {
		return
	}
	h.muListeners.Lock()
	for _, l := range h.listeners {
		if xreflect.IsSameFunc(l, listener) {
			h.muListeners.Unlock()
			return
		}
	}
	h.listeners = append(h.listeners, listener)
	h.muListeners.Unlock()
}

func (h *MsgHooker) RemoveListener(listener MsgHookListener) {
	if listener == nil {
		return
	}
	h.muListeners.Lock()
	for i, l := range h.listeners {
		if xreflect.IsSameFunc(l, listener) {
			h.listeners = append(h.listeners[:i], h.listeners[i+1:]...)
			break
		}
	}
	h.muListeners.Unlock()
}

func (h *MsgHooker) RemoveAllListeners() {
	h.muListeners.Lock()
	h.listeners = make([]MsgHookListener, 0)
	h.muListeners.Unlock()
}

func (h *MsgHooker) Notify(msgId kkpacket.MSGID, data any) {
	h.muListeners.RLock()
	listeners := make([]MsgHookListener, len(h.listeners))
	copy(listeners, h.listeners)
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
