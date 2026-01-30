package gnrpc

import (
	"context"
	"time"
)

type callConfig struct {
	timeout time.Duration
	headers map[string]string

	respHeaders  *MD
	respTrailers *MD
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

// WithHeader sets a request header key/value.
func WithHeader(k, v string) CallOption {
	return func(c *callConfig) {
		if k == "" {
			return
		}
		if c.headers == nil {
			c.headers = make(map[string]string)
		}
		c.headers[k] = v
	}
}

// WithResponseHeaders captures response headers into out (overwritten).
func WithResponseHeaders(out *MD) CallOption {
	return func(c *callConfig) {
		c.respHeaders = out
	}
}

// WithResponseTrailers captures response trailers into out (overwritten).
func WithResponseTrailers(out *MD) CallOption {
	return func(c *callConfig) {
		c.respTrailers = out
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

