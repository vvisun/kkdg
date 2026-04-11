package kktime

import (
	"sync"
	"time"

	"github.com/vvisun/kkdg/utils/timingwheel"
)

var (
	netTW          *timingwheel.TimingWheel
	onceNetTWStart sync.Once
	onceNetTWStop  sync.Once
	netTWTick            = 50 * time.Millisecond
	netTWWheelSize int64 = 500 // 500 * 50ms = 25s
)

var (
	gameTW          *timingwheel.TimingWheel
	onceGameTWStart sync.Once
	onceGameTWStop  sync.Once
	gameTWTick            = 1 * time.Millisecond
	gameTWWheelSize int64 = 100 // 100 * 1ms = 100ms
)

func GetNetTimingWheel() *timingwheel.TimingWheel {
	onceNetTWStart.Do(func() {
		netTW = timingwheel.NewTimingWheel(netTWTick, netTWWheelSize)
		netTW.Start()
	})
	return netTW
}

func StopNetTimingWheel() {
	onceNetTWStop.Do(func() {
		if netTW != nil {
			netTW.Stop()
			netTW = nil
		}
	})
}

func GetGameTimingWheel() *timingwheel.TimingWheel {
	onceGameTWStart.Do(func() {
		gameTW = timingwheel.NewTimingWheel(gameTWTick, gameTWWheelSize)
		gameTW.Start()
	})
	return gameTW
}

func StopGameTimingWheel() {
	onceGameTWStop.Do(func() {
		if gameTW != nil {
			gameTW.Stop()
			gameTW = nil
		}
	})
}

// ConfigureNetTimingWheel configures the net timing wheel.
// The default value is 500ms * 60 = 30s.
// must call this function before GetNetTimingWheel is called.
func ConfigureNetTimingWheel(tick time.Duration, wheelSize int64) {
	netTWTick = tick
	netTWWheelSize = wheelSize
}

// ConfigureGameTimingWheel configures the game timing wheel.
// The default value is 1ms * 20 = 20ms.
// must call this function before GetGameTimingWheel is called.
func ConfigureGameTimingWheel(tick time.Duration, wheelSize int64) {
	gameTWTick = tick
	gameTWWheelSize = wheelSize
}
