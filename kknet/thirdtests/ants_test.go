package thirdtests

import (
	"testing"
	"time"

	"github.com/panjf2000/ants/v2"
	"github.com/vvisun/kkdg/utils/kklog"
)

func TestAnts(t *testing.T) {
	pool, err := ants.NewPool(10, ants.WithOptions(ants.Options{
		MaxBlockingTasks: 0,
		Nonblocking:      true,
		PanicHandler: func(err any) {
			t.Errorf("panic error: %v", err)
		},
		Logger:         kklog.Stdout().(ants.Logger),
		DisablePurge:   true,
		ExpiryDuration: 1 * time.Second,
		PreAlloc:       true,
	}))
	if err != nil {
		t.Errorf("new pool error: %v", err)
	}

	pool.Submit(func() {
		t.Log("task")
	})

	time.Sleep(2 * time.Second)

	pool.Release()

	time.Sleep(2 * time.Second)

	pool.Submit(func() {
		t.Log("task")
	})
}
