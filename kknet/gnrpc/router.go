package gnrpc

import (
	"context"
	"sync"
)

type router struct {
	mu sync.RWMutex
	m  map[string]Handler
}

func newRouter() *router {
	return &router{m: make(map[string]Handler)}
}

func (r *router) Register(method string, h Handler) {
	if method == "" || h == nil {
		return
	}
	r.mu.Lock()
	r.m[method] = h
	r.mu.Unlock()
}

func (r *router) Call(ctx context.Context, method string, payload []byte) ([]byte, error) {
	r.mu.RLock()
	h := r.m[method]
	r.mu.RUnlock()
	if h == nil {
		return nil, ErrMethodNotFound
	}
	return h(ctx, payload)
}

