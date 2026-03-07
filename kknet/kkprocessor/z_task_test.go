package kkprocessor

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerQueue_ExecutesAllJobs(t *testing.T) {
	const N = 100
	wq := NewWorkerQueue(4)
	var count atomic.Int32
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		wq.Push(func() {
			count.Add(1)
			wg.Done()
		})
	}
	wg.Wait()
	if got := count.Load(); got != N {
		t.Errorf("executed %d jobs, want %d", got, N)
	}
}

func TestWorkerQueue_OrderWhenConcurrencyOne(t *testing.T) {
	const N = 500000
	wq := NewWorkerQueue(1)
	//var mu sync.Mutex
	var order []int = make([]int, 0, N)
	done := make(chan struct{})
	for i := 0; i < N; i++ {
		idx := i
		wq.Push(func() {
			//mu.Lock()
			order = append(order, idx)
			//mu.Unlock()
			if idx == N-1 {
				close(done)
			}
		})
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for all jobs")
	}
	//mu.Lock()
	got := order
	//mu.Unlock()
	if len(got) != N {
		t.Fatalf("executed %d jobs, want %d", len(got), N)
	}
	for i := 0; i < N; i++ {
		if got[i] != i {
			t.Errorf("order[%d]=%d, want %d", i, got[i], i)
		}
	}
}

func TestWorkerQueue_RespectsMaxConcurrency(t *testing.T) {
	const maxConc = 3
	const numJobs = 20
	wq := NewWorkerQueue(maxConc)
	var current atomic.Int32
	var maxSeen atomic.Int32
	var wg sync.WaitGroup
	wg.Add(numJobs)
	for i := 0; i < numJobs; i++ {
		wq.Push(func() {
			n := current.Add(1)
			defer func() {
				current.Add(-1)
				wg.Done()
			}()
			for {
				seen := maxSeen.Load()
				if n <= seen {
					break
				}
				if maxSeen.CompareAndSwap(seen, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
		})
	}
	wg.Wait()
	if m := maxSeen.Load(); m > maxConc {
		t.Errorf("max concurrency seen %d, want <= %d", m, maxConc)
	}
}

func TestWorkerQueue_PushNilNoPanic(t *testing.T) {
	wq := NewWorkerQueue(1)
	wq.Push(nil)
	wq.Push(nil)
	// 再推一个真实任务，确保队列能继续工作
	done := make(chan struct{})
	wq.Push(func() { close(done) })
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Error("job after nil pushes did not run")
	}
}
