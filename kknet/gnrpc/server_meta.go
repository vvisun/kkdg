package gnrpc

import "context"

type serverMetaKey struct{}

// ServerMeta holds response metadata for a single server-side call.
type ServerMeta struct {
	headers  MD
	trailers MD
}

func (m *ServerMeta) ensureHeaders() {
	if m.headers == nil {
		m.headers = make(MD)
	}
}
func (m *ServerMeta) ensureTrailers() {
	if m.trailers == nil {
		m.trailers = make(MD)
	}
}

// SetHeader sets a response header.
func SetHeader(ctx context.Context, k, v string) {
	if ctx == nil || k == "" {
		return
	}
	m, ok := ctx.Value(serverMetaKey{}).(*ServerMeta)
	if !ok || m == nil {
		return
	}
	m.ensureHeaders()
	m.headers[k] = v
}

// SetTrailer sets a response trailer.
func SetTrailer(ctx context.Context, k, v string) {
	if ctx == nil || k == "" {
		return
	}
	m, ok := ctx.Value(serverMetaKey{}).(*ServerMeta)
	if !ok || m == nil {
		return
	}
	m.ensureTrailers()
	m.trailers[k] = v
}

func withServerMeta(ctx context.Context) (context.Context, *ServerMeta) {
	if ctx == nil {
		ctx = context.Background()
	}
	meta := &ServerMeta{}
	return context.WithValue(ctx, serverMetaKey{}, meta), meta
}

