package kkrpc

import (
	"context"
	"sync"
)

// RpcHandler handles a unary RPC call.
// It receives the raw payload bytes and returns raw payload bytes.
type RpcHandler func(ctx context.Context, req []byte) ([]byte, error)

type RpcRouter struct {
	mu sync.RWMutex
	m  map[string]RpcHandler
}

func NewRouter() *RpcRouter {
	return &RpcRouter{m: make(map[string]RpcHandler)}
}

func (r *RpcRouter) Register(method string, h RpcHandler) {
	if method == "" || h == nil {
		return
	}
	r.mu.Lock()
	r.m[method] = h
	r.mu.Unlock()
}

func (r *RpcRouter) Call(ctx context.Context, method string, payload []byte) ([]byte, error) {
	r.mu.RLock()
	h := r.m[method]
	r.mu.RUnlock()
	if h == nil {
		return nil, ErrMethodNotFound
	}
	return h(ctx, payload)
}
