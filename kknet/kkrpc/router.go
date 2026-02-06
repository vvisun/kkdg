package kkrpc

import (
	"context"
	"sync"
)

// RpcHandler handles a unary RPC call.
// It receives the raw payload bytes and returns raw payload bytes.
type RpcHandler func(ctx context.Context, req []byte) ([]byte, error)

type RpcRouter struct {
	m sync.Map //  map[string]RpcHandler
}

func NewRouter() *RpcRouter {
	return &RpcRouter{}
}

func (r *RpcRouter) Register(method string, h RpcHandler) {
	if method == "" || h == nil {
		return
	}
	r.m.Store(method, h)
}

func (r *RpcRouter) Call(ctx context.Context, method string, payload []byte) ([]byte, error) {
	h, ok := r.m.Load(method)
	if !ok || h == nil {
		return nil, ErrMethodNotFound
	}
	return h.(RpcHandler)(ctx, payload)
}
