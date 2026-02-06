package kkrpc

import (
	"context"
)

// 消息接收器
type IMsgHandler interface {
	GetMethod() string                           // 获取消息ID
	OnMsg(ctx context.Context, msg []byte) error // 消息回调
}

type RouteHandler[T any] struct {
	call   func(ctx context.Context, msg *T) error
	method string
}

func (h *RouteHandler[T]) GetMethod() string {
	return h.method
}

func (h *RouteHandler[T]) OnMsg(ctx context.Context, msg []byte) error {
	var t T
	if err := rpcCodec.Unmarshal(msg, &t); err != nil {
		return err
	}
	return h.call(ctx, &t)
}

func NewMsgHandler[T any](method string, call func(ctx context.Context, msg *T) error) *RouteHandler[T] {
	var handler RouteHandler[T]
	handler.call = call
	handler.method = method
	return &handler
}

type RpcRouter struct {
	m map[string]IMsgHandler
}

func NewRouter() *RpcRouter {
	return &RpcRouter{
		m: make(map[string]IMsgHandler),
	}
}

func RegisterHandler[T any](router *RpcRouter, h *RouteHandler[T]) {
	if h == nil {
		return
	}
	router.m[h.GetMethod()] = h
}

func (r *RpcRouter) OnMsg(ctx context.Context, method string, msg []byte) error {
	h, ok := r.m[method]
	if !ok || h == nil {
		return ErrMethodNotFound
	}
	return h.OnMsg(ctx, msg)
}
