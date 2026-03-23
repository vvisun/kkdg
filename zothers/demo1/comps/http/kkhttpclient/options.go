package kkhttpclient

import "time"

type ClientOptions struct {
	BaseURL string

	Timeout time.Duration
	// If true, non-2xx responses are returned as errors.
	StrictStatus bool
}

func defaultClientOptions() ClientOptions {
	return ClientOptions{
		Timeout:      10 * time.Second,
		StrictStatus: true,
	}
}

type ClientOption func(*ClientOptions)

func WithBaseURL(baseURL string) ClientOption {
	return func(o *ClientOptions) {
		o.BaseURL = baseURL
	}
}

func WithTimeout(timeout time.Duration) ClientOption {
	return func(o *ClientOptions) {
		if timeout > 0 {
			o.Timeout = timeout
		}
	}
}

func WithStrictStatus(strict bool) ClientOption {
	return func(o *ClientOptions) {
		o.StrictStatus = strict
	}
}
