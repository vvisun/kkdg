package gnrpc

import "context"

// MD is a minimal metadata (headers) map.
type MD map[string]string

type mdKey struct{}

// NewIncomingContext attaches incoming metadata to ctx.
func NewIncomingContext(ctx context.Context, md MD) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if md == nil {
		return ctx
	}
	return context.WithValue(ctx, mdKey{}, md)
}

// FromIncomingContext returns metadata from ctx.
func FromIncomingContext(ctx context.Context) (MD, bool) {
	if ctx == nil {
		return nil, false
	}
	v := ctx.Value(mdKey{})
	if v == nil {
		return nil, false
	}
	md, ok := v.(MD)
	return md, ok
}

