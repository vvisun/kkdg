package kktime

import (
	"sync"
	"sync/atomic"
	"time"
)

const max_size_for_pool = 4096

// global pool of *time.Timer's. can be used by multiple goroutines concurrently.
var globalTimerPool timerPool

func GetGlobalTimerPool() *timerPool {
	return &globalTimerPool
}

// timerPool provides GC-able pooling of *time.Timer's.
// can be used by multiple goroutines concurrently.
type timerPool struct {
	p    sync.Pool
	size int64
}

// Get returns a timer that completes after the given duration.
func (tp *timerPool) Get(d time.Duration) *time.Timer {
	if t, ok := tp.p.Get().(*time.Timer); ok && t != nil {
		t.Reset(d)
		// 原子操作减少size
		if atomic.LoadInt64(&tp.size) > 0 {
			atomic.AddInt64(&tp.size, -1)
		}
		return t
	}

	return time.NewTimer(d)
}

// Put pools the given timer.
//
// There is no need to call t.Stop() before calling Put.
//
// Put will try to stop the timer before pooling. If the
// given timer already expired, Put will read the unreceived
// value if there is one.
func (tp *timerPool) Put(t *time.Timer) {
	if t == nil {
		return
	}

	if !t.Stop() {
		select {
		case <-t.C:
		default:
		}
	}

	if atomic.LoadInt64(&tp.size) > max_size_for_pool {
		return // 防止池过大耗尽内存
	}

	tp.p.Put(t)
	atomic.AddInt64(&tp.size, 1)
}
