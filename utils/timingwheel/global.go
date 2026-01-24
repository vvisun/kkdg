package timingwheel

import (
	"sync"
	"time"
)

var (
	globalTW          *TimingWheel
	onceGlobalTWStart sync.Once
	onceGlobalTWStop  sync.Once
)

func GetGlobalTimingWheel() *TimingWheel {
	onceGlobalTWStart.Do(func() {
		globalTW = NewTimingWheel(time.Millisecond, 20)
		globalTW.Start()
	})
	return globalTW
}

func StopGlobalTimingWheel() {
	onceGlobalTWStop.Do(func() {
		if globalTW != nil {
			globalTW.Stop()
			globalTW = nil
		}
	})
}
