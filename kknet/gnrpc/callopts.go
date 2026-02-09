package gnrpc

import (
	"context"
	"time"
)

type callConfig struct {
	timeout time.Duration
}

// CallOption configures a single Invoke call.
type CallOption func(*callConfig)

// WithTimeout applies a timeout if ctx has no deadline.
func WithTimeout(d time.Duration) CallOption {
	return func(c *callConfig) {
		if d > 0 {
			c.timeout = d
		}
	}
}

func applyCallOptions(opts []CallOption) callConfig {
	var cfg callConfig
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func maybeApplyTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	if _, ok := ctx.Deadline(); ok || d <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d)
}
