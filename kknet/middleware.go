package kknet

// Middleware is a function that wraps a IHandler to provide additional functionality.
// It receives the next IHandler in the chain and returns a new IHandler.
type Middleware func(IHandler) IHandler

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
	// Apply in reverse order so the first middleware is the outermost wrapper.
	h := handler
	for i := len(middlewares) - 1; i >= 0; i-- {
		mw := middlewares[i]
		if mw == nil {
			continue
		}
		h = mw(h)
	}
	return h
}
