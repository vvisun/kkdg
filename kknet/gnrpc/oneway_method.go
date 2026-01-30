package gnrpc

import (
	"sync"
	"sync/atomic"
	"time"
)

// MethodConfig configures per-method controls.
//
// It applies to both:
// - FrameTypeOneway (fire-and-forget)
// - FrameTypeRequest (regular request/response)
type MethodConfig struct {
	// MaxInFlight limits total outstanding tasks (queued + running) for this method.
	// <=0 means unlimited.
	MaxInFlight int64

	// TokenBucketRate is tokens per second. <=0 disables rate limit.
	TokenBucketRate float64
	// TokenBucketBurst is max burst tokens. <=0 means 1.
	TokenBucketBurst float64

	// BreakerEnabled enables circuit breaker.
	BreakerEnabled bool
	// BreakerTripFailures opens breaker after N consecutive failures.
	// <=0 means 1 when enabled.
	BreakerTripFailures int64
	// BreakerCooldown is how long to stay open before allowing a single trial.
	// <=0 means 1s when enabled.
	BreakerCooldown time.Duration
}

type MethodStats struct {
	Method    string
	Enqueued  int64
	Dropped   int64
	Processed int64
	Failed    int64

	RateLimited int64
	BreakerOpen int64
	InFlightMax int64
}

type methodOnewayState struct {
	cfg atomic.Value // stores MethodConfig

	inFlight atomic.Int64

	enq   atomic.Int64
	drop  atomic.Int64
	proc  atomic.Int64
	fail  atomic.Int64
	rl    atomic.Int64
	brk   atomic.Int64
	ifMax atomic.Int64

	tb  *tokenBucket
	cb  *circuitBreaker
	mux sync.Mutex // protects tb/cb init/update
}

func (s *methodOnewayState) setConfig(cfg MethodConfig) {
	s.cfg.Store(cfg)
	s.mux.Lock()
	// (re)create token bucket / breaker as needed
	if cfg.TokenBucketRate > 0 {
		burst := cfg.TokenBucketBurst
		if burst <= 0 {
			burst = 1
		}
		if s.tb == nil {
			s.tb = newTokenBucket(cfg.TokenBucketRate, burst)
		} else {
			s.tb.Set(cfg.TokenBucketRate, burst)
		}
	} else {
		s.tb = nil
	}
	if cfg.BreakerEnabled {
		trip := cfg.BreakerTripFailures
		if trip <= 0 {
			trip = 1
		}
		cool := cfg.BreakerCooldown
		if cool <= 0 {
			cool = 1 * time.Second
		}
		if s.cb == nil {
			s.cb = newCircuitBreaker(trip, cool)
		} else {
			s.cb.Set(trip, cool)
		}
	} else {
		s.cb = nil
	}
	s.mux.Unlock()
}

func (s *methodOnewayState) getConfig() MethodConfig {
	v := s.cfg.Load()
	if v == nil {
		return MethodConfig{}
	}
	return v.(MethodConfig)
}

func (s *methodOnewayState) tryAcquire() (ok bool, reason string) {
	cfg := s.getConfig()

	// rate limit
	if cfg.TokenBucketRate > 0 {
		s.mux.Lock()
		tb := s.tb
		s.mux.Unlock()
		if tb != nil && !tb.Allow() {
			s.rl.Add(1)
			s.drop.Add(1)
			return false, "rate"
		}
	}

	// breaker
	if cfg.BreakerEnabled {
		s.mux.Lock()
		cb := s.cb
		s.mux.Unlock()
		if cb != nil && !cb.Allow() {
			s.brk.Add(1)
			s.drop.Add(1)
			return false, "breaker"
		}
	}

	// in-flight limit
	if cfg.MaxInFlight > 0 {
		n := s.inFlight.Add(1)
		if n > cfg.MaxInFlight {
			s.inFlight.Add(-1)
			s.ifMax.Add(1)
			s.drop.Add(1)
			return false, "inflight"
		}
		return true, ""
	}

	s.inFlight.Add(1)
	return true, ""
}

func (s *methodOnewayState) onEnqueued() {
	s.enq.Add(1)
}

func (s *methodOnewayState) onDropped() {
	s.drop.Add(1)
	s.inFlight.Add(-1)
}

func (s *methodOnewayState) onProcessed(err error) {
	s.proc.Add(1)
	if err != nil {
		s.fail.Add(1)
		s.mux.Lock()
		cb := s.cb
		s.mux.Unlock()
		if cb != nil {
			cb.Fail()
		}
	} else {
		s.mux.Lock()
		cb := s.cb
		s.mux.Unlock()
		if cb != nil {
			cb.Success()
		}
	}
	s.inFlight.Add(-1)
}

func (s *methodOnewayState) snapshot(method string) MethodStats {
	cfg := s.getConfig()
	st := MethodStats{
		Method:      method,
		Enqueued:    s.enq.Load(),
		Dropped:     s.drop.Load(),
		Processed:   s.proc.Load(),
		Failed:      s.fail.Load(),
		RateLimited: s.rl.Load(),
		BreakerOpen: s.brk.Load(),
		InFlightMax: s.ifMax.Load(),
	}
	_ = cfg
	return st
}

// ---- token bucket ----

type tokenBucket struct {
	mu     sync.Mutex
	rate   float64
	burst  float64
	tokens float64
	last   time.Time
}

func newTokenBucket(rate, burst float64) *tokenBucket {
	now := time.Now()
	return &tokenBucket{
		rate:   rate,
		burst: burst,
		tokens: burst,
		last:   now,
	}
}

func (tb *tokenBucket) Set(rate, burst float64) {
	tb.mu.Lock()
	tb.rate = rate
	tb.burst = burst
	if tb.tokens > burst {
		tb.tokens = burst
	}
	tb.last = time.Now()
	tb.mu.Unlock()
}

func (tb *tokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(tb.last).Seconds()
	if elapsed > 0 {
		tb.tokens += elapsed * tb.rate
		if tb.tokens > tb.burst {
			tb.tokens = tb.burst
		}
		tb.last = now
	}
	if tb.tokens >= 1 {
		tb.tokens -= 1
		return true
	}
	return false
}

// ---- circuit breaker ----

type breakerState int

const (
	breakerClosed breakerState = iota
	breakerOpen
	breakerHalfOpen
)

type circuitBreaker struct {
	mu        sync.Mutex
	trip      int64
	cooldown  time.Duration
	state     breakerState
	openedAt  time.Time
	failCount int64
	// half-open allows one trial at a time
	trialInFlight bool
}

func newCircuitBreaker(trip int64, cooldown time.Duration) *circuitBreaker {
	return &circuitBreaker{
		trip:     trip,
		cooldown: cooldown,
		state:    breakerClosed,
	}
}

func (cb *circuitBreaker) Set(trip int64, cooldown time.Duration) {
	cb.mu.Lock()
	cb.trip = trip
	cb.cooldown = cooldown
	cb.mu.Unlock()
}

func (cb *circuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	switch cb.state {
	case breakerClosed:
		return true
	case breakerOpen:
		if time.Since(cb.openedAt) >= cb.cooldown {
			cb.state = breakerHalfOpen
			cb.trialInFlight = false
		} else {
			return false
		}
		fallthrough
	case breakerHalfOpen:
		if cb.trialInFlight {
			return false
		}
		cb.trialInFlight = true
		return true
	default:
		return true
	}
}

func (cb *circuitBreaker) Success() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failCount = 0
	cb.trialInFlight = false
	cb.state = breakerClosed
}

func (cb *circuitBreaker) Fail() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.trialInFlight = false
	cb.failCount++
	if cb.state == breakerHalfOpen {
		cb.state = breakerOpen
		cb.openedAt = time.Now()
		cb.failCount = cb.trip
		return
	}
	if cb.failCount >= cb.trip {
		cb.state = breakerOpen
		cb.openedAt = time.Now()
	}
}

