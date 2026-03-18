package kktime

import (
	"sync"
	"time"

	"github.com/vvisun/kkdg/utils/timingwheel"
)

type TimerWrapper struct {
	tw         *timingwheel.TimingWheel
	timerMap   map[uint64]*timingwheel.Timer
	muTimerMap sync.RWMutex
}

func newTimerWrapper(tw *timingwheel.TimingWheel) *TimerWrapper {
	return &TimerWrapper{
		tw:         tw,
		timerMap:   make(map[uint64]*timingwheel.Timer),
		muTimerMap: sync.RWMutex{},
	}
}

func NewGameTimerWrapper() *TimerWrapper {
	return newTimerWrapper(GetGameTimingWheel())
}

func NewNetTimerWrapper() *TimerWrapper {
	return newTimerWrapper(GetNetTimingWheel())
}

func (w *TimerWrapper) AfterFunc(d time.Duration, f func()) uint64 {
	t := w.tw.AfterFunc(d, f)
	w.muTimerMap.Lock()
	w.timerMap[t.GetID()] = t
	w.muTimerMap.Unlock()
	return t.GetID()
}

func (w *TimerWrapper) ScheduleFunc(s timingwheel.Scheduler, f func()) uint64 {
	t := w.tw.ScheduleFunc(s, f)
	w.muTimerMap.Lock()
	w.timerMap[t.GetID()] = t
	w.muTimerMap.Unlock()
	return t.GetID()
}

func (w *TimerWrapper) StopTimer(id uint64) {
	w.muTimerMap.Lock()
	t, ok := w.timerMap[id]
	if ok {
		t.Stop()
		delete(w.timerMap, id)
	}
	w.muTimerMap.Unlock()
}

func (w *TimerWrapper) GetTimer(id uint64) *timingwheel.Timer {
	w.muTimerMap.RLock()
	t, ok := w.timerMap[id]
	if ok {
		w.muTimerMap.RUnlock()
		return t
	}
	w.muTimerMap.RUnlock()
	return nil
}

func (w *TimerWrapper) Stop() {
	w.muTimerMap.Lock()
	if len(w.timerMap) == 0 {
		w.muTimerMap.Unlock()
		return
	}
	timerList := make([]*timingwheel.Timer, 0, len(w.timerMap))
	for _, t := range w.timerMap {
		timerList = append(timerList, t)
	}
	w.timerMap = make(map[uint64]*timingwheel.Timer)
	w.muTimerMap.Unlock()
	for _, t := range timerList {
		t.Stop()
	}
}
