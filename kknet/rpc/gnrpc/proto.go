package gnrpc

import (
	"context"

	"google.golang.org/protobuf/proto"
)

// RegisterProto registers a protobuf unary handler.
//
// - newReq: must return a new *T instance each time.
// - handler: takes decoded request message and returns response message.
func (s *Server) RegisterProto(method string, newReq func() proto.Message, handler func(ctx context.Context, req proto.Message) (proto.Message, error)) {
	if newReq == nil || handler == nil {
		return
	}
	s.Register(method, func(ctx context.Context, reqBytes []byte) ([]byte, error) {
		req := newReq()
		if req == nil {
			return nil, Status(CodeInternal, "nil request factory")
		}
		if len(reqBytes) > 0 {
			if err := proto.Unmarshal(reqBytes, req); err != nil {
				return nil, Status(CodeInvalidArgument, err.Error())
			}
		}
		resp, err := handler(ctx, req)
		if err != nil {
			return nil, err
		}
		if resp == nil {
			return nil, nil
		}
		b, err := proto.Marshal(resp)
		if err != nil {
			return nil, Status(CodeInternal, err.Error())
		}
		return b, nil
	})
}

// InvokeProto performs a protobuf unary call and decodes the response into resp.
// If resp is nil, it just executes the call.
func (c *Client) InvokeProto(ctx context.Context, method string, req proto.Message, resp proto.Message) error {
	var reqBytes []byte
	if req != nil {
		b, err := proto.Marshal(req)
		if err != nil {
			return err
		}
		reqBytes = b
	}
	out, err := c.Invoke(ctx, method, reqBytes)
	if err != nil {
		return err
	}
	if resp == nil {
		return nil
	}
	if len(out) == 0 {
		return nil
	}
	if err := proto.Unmarshal(out, resp); err != nil {
		return err
	}
	return nil
}

