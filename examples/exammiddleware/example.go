package exammiddleware

import (
	"fmt"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kklog"
)

// EchoHandler is a simple echo server handler.
type EchoHandler struct{}

func (h *EchoHandler) OnConnect(c kknet.IConn) {
	fmt.Printf("Echo: Client %d connected\n", c.ID())
}

func (h *EchoHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	// Echo the message back
	if err := c.SendBuffer(data); err != nil {
		fmt.Printf("Echo: Failed to send: %v\n", err)
	}
}

func (h *EchoHandler) OnClose(c kknet.IConn, err error) {
	fmt.Printf("Echo: Client %d disconnected\n", c.ID())
}

func RunMain() {
	// Create a TCP server with middleware
	handler := &EchoHandler{}
	server := kktcp.NewServer(":8080", handler,
		kknet.WithLogger(kklog.Stdout()),
		kknet.WithMiddleware(LoggingMiddleware),
		kknet.WithMiddleware(RateLimitMiddleware(10)),
		kknet.WithMiddleware(AuthMiddleware(map[string]bool{
			"token123": true,
			"token456": true,
		})),
	)

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
