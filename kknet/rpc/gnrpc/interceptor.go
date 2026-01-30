package gnrpc

import "context"

// UnaryServerInterceptor intercepts server-side unary calls.
// It receives the raw request payload and returns raw response payload.
type UnaryServerInterceptor func(ctx context.Context, method string, req []byte, handler Handler) ([]byte, error)

// UnaryClientInterceptor intercepts client-side unary calls.
type UnaryClientInterceptor func(ctx context.Context, method string, req []byte, invoker Invoker) ([]byte, error)

// Invoker performs the actual RPC call on client.
type Invoker func(ctx context.Context, method string, req []byte) ([]byte, error)

func chainServerInterceptors(interceptors []UnaryServerInterceptor, final Handler, method string) Handler {
	if len(interceptors) == 0 {
		return final
	}
	h := final
	for i := len(interceptors) - 1; i >= 0; i-- {
		ic := interceptors[i]
		next := h
		h = func(ctx context.Context, req []byte) ([]byte, error) {
			return ic(ctx, method, req, next)
		}
	}
	return h
}

func chainClientInterceptors(interceptors []UnaryClientInterceptor, inv Invoker) Invoker {
	if len(interceptors) == 0 {
		return inv
	}
	out := inv
	for i := len(interceptors) - 1; i >= 0; i-- {
		ic := interceptors[i]
		next := out
		out = func(ctx context.Context, method string, req []byte) ([]byte, error) {
			return ic(ctx, method, req, next)
		}
	}
	return out
}

