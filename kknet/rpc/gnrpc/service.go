package gnrpc

import (
	"context"
	"strings"

	"google.golang.org/protobuf/proto"
)

// FullMethodName returns a canonical gRPC-like full method name:
// "/<service>/<method>".
//
// It accepts inputs with or without leading/trailing slashes.
func FullMethodName(service, method string) string {
	s := strings.Trim(service, "/")
	m := strings.Trim(method, "/")
	if s == "" {
		// Keep compatible with legacy direct registration.
		return "/" + m
	}
	if m == "" {
		return "/" + s
	}
	return "/" + s + "/" + m
}

// UnaryMethodDesc describes a unary method.
type UnaryMethodDesc struct {
	MethodName string
	Handler    Handler
}

// ServiceDesc describes a unary RPC service.
type ServiceDesc struct {
	ServiceName string
	Methods     []UnaryMethodDesc
}

// RegisterService registers methods using canonical full method names.
func (s *Server) RegisterService(desc ServiceDesc) {
	if desc.ServiceName == "" {
		return
	}
	for _, m := range desc.Methods {
		if m.MethodName == "" || m.Handler == nil {
			continue
		}
		s.Register(FullMethodName(desc.ServiceName, m.MethodName), m.Handler)
	}
}

// UnaryProtoMethodDesc describes a protobuf unary method.
type UnaryProtoMethodDesc struct {
	MethodName string
	NewRequest func() proto.Message
	Handler    func(ctx context.Context, req proto.Message) (proto.Message, error)
}

// ProtoServiceDesc describes a protobuf unary RPC service.
type ProtoServiceDesc struct {
	ServiceName string
	Methods     []UnaryProtoMethodDesc
}

// RegisterProtoService registers protobuf methods using canonical full method names.
func (s *Server) RegisterProtoService(desc ProtoServiceDesc) {
	if desc.ServiceName == "" {
		return
	}
	for _, m := range desc.Methods {
		if m.MethodName == "" || m.NewRequest == nil || m.Handler == nil {
			continue
		}
		s.RegisterProto(FullMethodName(desc.ServiceName, m.MethodName), m.NewRequest, m.Handler)
	}
}

// InvokeService calls "/service/method" using raw bytes.
func (c *Client) InvokeService(ctx context.Context, service, method string, req []byte, opts ...CallOption) ([]byte, error) {
	return c.Invoke(ctx, FullMethodName(service, method), req, opts...)
}

// InvokeProtoService calls "/service/method" using protobuf messages.
func (c *Client) InvokeProtoService(ctx context.Context, service, method string, req proto.Message, resp proto.Message, opts ...CallOption) error {
	return c.InvokeProtoWithOptions(ctx, FullMethodName(service, method), req, resp, opts...)
}

