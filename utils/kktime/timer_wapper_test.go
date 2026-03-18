package kktime

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/vvisun/kkdg/utils/timingwheel"
)

type everyScheduler struct {
	Interval time.Duration
}

func (s *everyScheduler) Next(prev time.Time) time.Time {
	return prev.Add(s.Interval)
}

func TestTimerWrapper_AfterFunc_FiresAndGetTimer(t *testing.T) {
	tw := timingwheel.NewTimingWheel(time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	w := newTimerWrapper(tw)

	fired := make(chan struct{}, 1)
	id := w.AfterFunc(10*time.Millisecond, func() { fired <- struct{}{} })
	if id == 0 {
		t.Fatalf("expected non-zero timer id")
	}
	if w.GetTimer(id) == nil {
		t.Fatalf("expected GetTimer(%d) non-nil right after AfterFunc", id)
	}

	select {
	case <-fired:
		// ok
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("timer didn't fire in time")
	}
}

func TestTimerWrapper_StopTimer_RemovesAndStops(t *testing.T) {
	tw := timingwheel.NewTimingWheel(time.Millisecond, 20)
	tw.Start()
	defer tw.Stop()

	w := newTimerWrapper(tw)

	var called atomic.Int32
	id := w.AfterFunc(200*time.Millisecond, func() { called.Add(1) })
	if w.GetTimer(id) == nil {
		t.Fatalf("expected timer present before StopTimer")
	}

	w.StopTimer(id)
	if w.GetTimer(id) != nil {
		t.Fatalf("expected timer removed after StopTimer")
	}

	time.Sleep(250 * time.Millisecond)
	if called.Load() != 0 {
		t.Fatalf("expected timer not to fire after StopTimer, got %d", called.Load())
	}
}

func TestTimerWrapper_ScheduleFunc_FiresAndCanStop(t *testing.T) {
	tw := timingwheel.NewTimingWheel(time.Millisecond, 50)
	tw.Start()
	defer tw.Stop()

	w := newTimerWrapper(tw)

	var called atomic.Int32
	fireC := make(chan struct{}, 8)

	id := w.ScheduleFunc(&everyScheduler{Interval: 10 * time.Millisecond}, func() {
		called.Add(1)
		fireC <- struct{}{}
	})
	if id == 0 {
		t.Fatalf("expected non-zero timer id")
	}
	if w.GetTimer(id) == nil {
		t.Fatalf("expected GetTimer(%d) non-nil right after ScheduleFunc", id)
	}

	// Wait for at least 2 fires.
	select {
	case <-fireC:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected scheduler timer to fire")
	}
	select {
	case <-fireC:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("expected scheduler timer to fire second time")
	}

	w.StopTimer(id)
	if w.GetTimer(id) != nil {
		t.Fatalf("expected timer removed after StopTimer")
	}

	// Ensure it doesn't keep firing after stop (best-effort due to race window).
	prev := called.Load()
	time.Sleep(50 * time.Millisecond)
	after := called.Load()
	if after > prev+1 {
		t.Fatalf("expected no more fires after stop (allow 1 in-flight), before=%d after=%d", prev, after)
	}
}

func TestTimerWrapper_Stop_StopsAndClearsAll(t *testing.T) {
	tw := timingwheel.NewTimingWheel(time.Millisecond, 50)
	tw.Start()
	defer tw.Stop()

	w := newTimerWrapper(tw)

	var called atomic.Int32
	id1 := w.AfterFunc(200*time.Millisecond, func() { called.Add(1) })
	id2 := w.AfterFunc(200*time.Millisecond, func() { called.Add(1) })

	if w.GetTimer(id1) == nil || w.GetTimer(id2) == nil {
		t.Fatalf("expected timers present before Stop")
	}

	w.Stop()
	if w.GetTimer(id1) != nil || w.GetTimer(id2) != nil {
		t.Fatalf("expected all timers cleared after Stop")
	}

	time.Sleep(250 * time.Millisecond)
	if called.Load() != 0 {
		t.Fatalf("expected timers not to fire after Stop, got %d", called.Load())
	}
}

