package xrand_test

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vvisun/kkdg/utils/xconv"
	"github.com/vvisun/kkdg/utils/xrand"
)

func TestConcurrentAPIs(t *testing.T) {
	const (
		workers = 64
		loops   = 2000
	)

	var okCount atomic.Int64
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < loops; j++ {
				if s := xrand.Letters(16); len(s) != 16 {
					t.Errorf("Letters length = %d, want 16", len(s))
					return
				}
				if n := xrand.Int(1, 100); n < 1 || n > 100 {
					t.Errorf("Int out of range: %d", n)
					return
				}
				if v := xrand.Float32(-50, 50); v < -50 || v >= 50 {
					t.Errorf("Float32 out of range: %f", v)
					return
				}
				_ = xrand.Lucky(33.3)
				_ = xrand.Weight(func(v any) float64 { return xconv.Float64(v) }, 50, 20.3, 29.7)
				okCount.Add(1)
			}
		}(i)
	}

	wg.Wait()
	want := int64(workers * loops)
	if okCount.Load() != want {
		t.Fatalf("concurrent run count = %d, want %d", okCount.Load(), want)
	}
}
