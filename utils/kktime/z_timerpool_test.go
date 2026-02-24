package kktime

import (
	"sync"
	"testing"
	"time"
)

func TestTimerPool_Get_FiresAfterDuration(t *testing.T) {
	pool := GetGlobalTimerPool()
	d := 30 * time.Millisecond
	timer := pool.Get(d)
	start := time.Now()
	<-timer.C
	elapsed := time.Since(start)
	pool.Put(timer)
	if elapsed < d/2 {
		t.Errorf("timer fired too early: %v", elapsed)
	}
	if elapsed > d*2 {
		t.Errorf("timer fired too late: %v", elapsed)
	}
}

func TestTimerPool_Put_Nil(t *testing.T) {
	pool := GetGlobalTimerPool()
	pool.Put(nil) // should not panic
}

func TestTimerPool_Put_AfterStop(t *testing.T) {
	pool := GetGlobalTimerPool()
	timer := pool.Get(time.Hour)
	timer.Stop()
	pool.Put(timer) // should not panic
}

func TestTimerPool_Put_AfterFire(t *testing.T) {
	pool := GetGlobalTimerPool()
	timer := pool.Get(5 * time.Millisecond)
	<-timer.C
	pool.Put(timer) // Put 会 drain 已过期的 timer，不应 panic
}

func TestTimerPool_GetPut_Reuse(t *testing.T) {
	pool := GetGlobalTimerPool()
	t1 := pool.Get(10 * time.Millisecond)
	<-t1.C
	pool.Put(t1)

	t2 := pool.Get(20 * time.Millisecond)
	start := time.Now()
	<-t2.C
	elapsed := time.Since(start)
	pool.Put(t2)
	// 若复用 t1，Reset(20ms) 后应在约 20ms 后触发
	if elapsed < 10*time.Millisecond || elapsed > 50*time.Millisecond {
		t.Logf("elapsed (may reuse): %v", elapsed)
	}
}

func TestTimerPool_Concurrent(t *testing.T) {
	pool := GetGlobalTimerPool()
	const N = 50
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				d := time.Duration(5+j) * time.Millisecond
				timer := pool.Get(d)
				<-timer.C
				pool.Put(timer)
			}
		}()
	}
	wg.Wait()
}

func TestTimerPool_Get_ZeroDuration(t *testing.T) {
	pool := GetGlobalTimerPool()
	timer := pool.Get(0)
	select {
	case <-timer.C:
		// 0 duration 会立即或很快触发
	case <-time.After(100 * time.Millisecond):
		t.Error("timer with 0 duration did not fire")
	}
	pool.Put(timer)
}
