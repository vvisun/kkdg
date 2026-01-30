package gnrpc

import (
	"context"

	"github.com/vvisun/kkdg/kknet"
)

// Invoker is a small abstraction over "something that can invoke a method".
//
// Typical implementations:
// - ClientInvoker: client -> server (address-based)
// - ConnInvoker: server -> client (connection-based)
type Invoker interface {
	Invoke(ctx context.Context, method string, req []byte, opts ...CallOption) ([]byte, error)
	InvokeNoResponse(ctx context.Context, method string, req []byte, opts ...CallOption) error
}

// ClientInvoker adapts *Client to Invoker.
type ClientInvoker struct{ C *Client }

func (i ClientInvoker) Invoke(ctx context.Context, method string, req []byte, opts ...CallOption) ([]byte, error) {
	if i.C == nil {
		return nil, ErrClientNotConnected
	}
	return i.C.Invoke(ctx, method, req, opts...)
}

func (i ClientInvoker) InvokeNoResponse(ctx context.Context, method string, req []byte, opts ...CallOption) error {
	if i.C == nil {
		return ErrClientNotConnected
	}
	return i.C.InvokeNoResponse(ctx, method, req, opts...)
}

// ConnInvoker adapts (Server + ConnID) to Invoker, enabling server-initiated calls to a connected peer.
type ConnInvoker struct {
	S      *Server
	ConnID kknet.CONN_ID
}

func (i ConnInvoker) Invoke(ctx context.Context, method string, req []byte, opts ...CallOption) ([]byte, error) {
	if i.S == nil {
		return nil, ErrClientNotConnected
	}
	return i.S.InvokeConn(ctx, i.ConnID, method, req, opts...)
}

func (i ConnInvoker) InvokeNoResponse(ctx context.Context, method string, req []byte, opts ...CallOption) error {
	if i.S == nil {
		return ErrClientNotConnected
	}
	return i.S.InvokeConnNoResponse(ctx, i.ConnID, method, req, opts...)
}
