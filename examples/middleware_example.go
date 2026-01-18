package main

import (
	"fmt"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kklog"
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

// RateLimitMiddleware limits message processing rate.
func RateLimitMiddleware(maxPerSecond int) kknet.Middleware {
	lastTime := time.Now()
	count := 0
	return func(next kknet.IHandler) kknet.IHandler {
		return &rateLimitHandler{
			next:         next,
			maxPerSecond: maxPerSecond,
			lastTime:     &lastTime,
			count:        &count,
		}
	}
}

type rateLimitHandler struct {
	next         kknet.IHandler
	maxPerSecond int
	lastTime     *time.Time
	count        *int
}

func (h *rateLimitHandler) OnConnect(c kknet.IConn) {
	if h.next != nil {
		h.next.OnConnect(c)
	}
}

func (h *rateLimitHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	now := time.Now()
	if now.Sub(*h.lastTime) >= time.Second {
		*h.lastTime = now
		*h.count = 0
	}
	if *h.count >= h.maxPerSecond {
		fmt.Printf("[RATE LIMIT] Connection %d exceeded rate limit\n", c.ID())
		return
	}
	*h.count++
	if h.next != nil {
		h.next.OnMessage(c, data)
	}
}

func (h *rateLimitHandler) OnClose(c kknet.IConn, err error) {
	if h.next != nil {
		h.next.OnClose(c, err)
	}
}

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

// EchoHandler is a simple echo server handler.
type EchoHandler struct{}

func (h *EchoHandler) OnConnect(c kknet.IConn) {
	fmt.Printf("Echo: Client %d connected\n", c.ID())
}

func (h *EchoHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	// Echo the message back
	if err := c.Send(data.B); err != nil {
		fmt.Printf("Echo: Failed to send: %v\n", err)
	}
}

func (h *EchoHandler) OnClose(c kknet.IConn, err error) {
	fmt.Printf("Echo: Client %d disconnected\n", c.ID())
}

func main() {
	// Create a TCP server with middleware
	handler := &EchoHandler{}
	server := kktcp.NewServer(":8080", handler,
		kknet.WithLogger(kklog.Stdout()),
	)

	// Add middlewares in order
	// They will be applied in reverse order (last added is outermost)
	server.Use(LoggingMiddleware)
	server.Use(RateLimitMiddleware(10)) // Max 10 messages per second
	server.Use(AuthMiddleware(map[string]bool{
		"token123": true,
		"token456": true,
	}))

	// Start the server
	if err := server.Start(); err != nil {
		fmt.Printf("Failed to start server: %v\n", err)
		return
	}

	fmt.Println("TCP server started on :8080 with middleware support")
	fmt.Println("Press Ctrl+C to stop...")

	// Keep the server running
	select {}
}
