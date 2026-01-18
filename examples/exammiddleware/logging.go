package exammiddleware

import (
	"fmt"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
)

// LoggingMiddleware logs connection events and messages.
func LoggingMiddleware(next kknet.IHandler) kknet.IHandler {
	return &loggingHandler{next: next}
}

type loggingHandler struct {
	next kknet.IHandler
}

func (h *loggingHandler) OnConnect(c kknet.IConn) {
	fmt.Printf("[LOG] Connection %d from %s established\n", c.ID(), c.RemoteAddr())
	if h.next != nil {
		h.next.OnConnect(c)
	}
}

func (h *loggingHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	fmt.Printf("[LOG] Message from connection %d: %d bytes\n", c.ID(), len(data.B))
	if h.next != nil {
		h.next.OnMessage(c, data)
	}
}

func (h *loggingHandler) OnClose(c kknet.IConn, err error) {
	if err != nil {
		fmt.Printf("[LOG] Connection %d closed with error: %v\n", c.ID(), err)
	} else {
		fmt.Printf("[LOG] Connection %d closed normally\n", c.ID())
	}
	if h.next != nil {
		h.next.OnClose(c, err)
	}
}
