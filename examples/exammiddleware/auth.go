package exammiddleware

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
)

// AuthMiddleware demonstrates authentication using connection context.
func AuthMiddleware(validTokens map[string]bool) kknet.Middleware {
	return func(next kknet.IHandler) kknet.IHandler {
		return &authHandler{
			next:        next,
			validTokens: validTokens,
		}
	}
}

type authHandler struct {
	next        kknet.IHandler
	validTokens map[string]bool
}

func (h *authHandler) OnConnect(c kknet.IConn) {
	// In a real implementation, you would extract token from handshake
	// For this example, we'll just allow all connections
	if h.next != nil {
		h.next.OnConnect(c)
	}
}

func (h *authHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	// In a real implementation, you would validate token from message
	// For this example, we'll just pass through
	if h.next != nil {
		h.next.OnMessage(c, data)
	}
}

func (h *authHandler) OnClose(c kknet.IConn, err error) {
	if h.next != nil {
		h.next.OnClose(c, err)
	}
}
