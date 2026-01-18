package kknet

import (
	"github.com/vvisun/kkdg/utils/buffers"
)

// Middleware is a function that wraps a IHandler to provide additional functionality.
// It receives the next IHandler in the chain and returns a new IHandler.
type Middleware func(IHandler) IHandler

// middlewareChain chains multiple middlewares together.
type middlewareChain struct {
	middlewares []Middleware
	handler     IHandler
}

// newMiddlewareChain creates a new middleware chain.
func newMiddlewareChain(handler IHandler, middlewares []Middleware) IHandler {
	if len(middlewares) == 0 {
		return handler
	}

	// Apply middlewares in reverse order so the first middleware
	// in the slice is the outermost wrapper.
	h := handler
	for i := len(middlewares) - 1; i >= 0; i-- {
		if middlewares[i] != nil {
			h = middlewares[i](h)
		}
	}
	return h
}

// handlerWrapper wraps a Handler with middleware support.
type handlerWrapper struct {
	handler IHandler
}

// OnConnect implements Handler.
func (w *handlerWrapper) OnConnect(c IConn) {
	if w.handler != nil {
		w.handler.OnConnect(c)
	}
}

// OnMessage implements Handler.
func (w *handlerWrapper) OnMessage(c IConn, data buffers.IBuffer) {
	if w.handler != nil {
		w.handler.OnMessage(c, data)
	}
}

// OnClose implements Handler.
func (w *handlerWrapper) OnClose(c IConn, err error) {
	if w.handler != nil {
		w.handler.OnClose(c, err)
	}
}

// ApplyMiddlewares applies a chain of middlewares to a handler.
// If handler is nil, returns nil.
// If middlewares is empty, returns the original handler.
func ApplyMiddlewares(handler IHandler, middlewares ...Middleware) IHandler {
	if handler == nil {
		return nil
	}
	if len(middlewares) == 0 {
		return handler
	}
	return newMiddlewareChain(handler, middlewares)
}
