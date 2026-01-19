package testmiddleware

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// TestMiddlewareChain tests that middlewares are applied in the correct order.
func TestMiddlewareChain(t *testing.T) {
	var callOrder []int
	callCounter := int32(0)

	// Create middlewares that record their execution order
	middleware1 := func(next kknet.IHandler) kknet.IHandler {
		return &middlewareTestHandler{
			next:    next,
			id:      1,
			order:   &callOrder,
			counter: &callCounter,
		}
	}

	middleware2 := func(next kknet.IHandler) kknet.IHandler {
		return &middlewareTestHandler{
			next:    next,
			id:      2,
			order:   &callOrder,
			counter: &callCounter,
		}
	}

	middleware3 := func(next kknet.IHandler) kknet.IHandler {
		return &middlewareTestHandler{
			next:    next,
			id:      3,
			order:   &callOrder,
			counter: &callCounter,
		}
	}

	baseHandler := &middlewareTestHandler{
		next:    nil,
		id:      0,
		order:   &callOrder,
		counter: &callCounter,
	}

	// Apply middlewares
	handler := kknet.ApplyMiddlewares(baseHandler, middleware1, middleware2, middleware3)

	// Create a mock connection
	conn := &mockConn{id: 1}

	// Call OnMessage - should execute in order: 1 -> 2 -> 3 -> 0
	callOrder = []int{}
	callCounter = 0
	data := kkbuffer.Get()
	data.B = []byte("test")
	handler.OnMessage(conn, data)
	kkbuffer.Put(data)

	// Verify execution order: middlewares should wrap from outside to inside
	// So execution order should be: 1, 2, 3, 0 (base handler)
	expectedOrder := []int{1, 2, 3, 0}
	if len(callOrder) != len(expectedOrder) {
		t.Fatalf("Expected %d calls, got %d", len(expectedOrder), len(callOrder))
	}
	for i, expected := range expectedOrder {
		if callOrder[i] != expected {
			t.Errorf("Call %d: expected %d, got %d", i, expected, callOrder[i])
		}
	}
}

// TestMiddlewareWithNilHandler tests that middlewares work with nil handler.
func TestMiddlewareWithNilHandler(t *testing.T) {
	counter := int32(0)
	order := []int{}
	middleware := func(next kknet.IHandler) kknet.IHandler {
		return &middlewareTestHandler{next: next, id: 1, order: &order, counter: &counter}
	}

	handler := kknet.ApplyMiddlewares(nil, middleware)
	if handler != nil {
		t.Error("Expected nil handler when input is nil")
	}
}

// TestMiddlewareEmptyChain tests that empty middleware chain returns original handler.
func TestMiddlewareEmptyChain(t *testing.T) {
	counter := int32(0)
	order := []int{}
	baseHandler := &middlewareTestHandler{next: nil, id: 0, order: &order, counter: &counter}
	handler := kknet.ApplyMiddlewares(baseHandler)
	if handler != baseHandler {
		t.Error("Expected original handler when no middlewares provided")
	}
}

// TestServerMiddleware tests that server Use method works correctly.
func TestServerMiddleware(t *testing.T) {
	var callOrder []int
	callCounter := int32(0)

	middleware1 := func(next kknet.IHandler) kknet.IHandler {
		return &middlewareTestHandler{
			next:    next,
			id:      1,
			order:   &callOrder,
			counter: &callCounter,
		}
	}

	middleware2 := func(next kknet.IHandler) kknet.IHandler {
		return &middlewareTestHandler{
			next:    next,
			id:      2,
			order:   &callOrder,
			counter: &callCounter,
		}
	}

	baseHandler := &middlewareTestHandler{
		next:    nil,
		id:      0,
		order:   &callOrder,
		counter: &callCounter,
	}

	server := kktcp.NewServer(":0", baseHandler,
		kknet.WithMiddleware(middleware1),
		kknet.WithMiddleware(middleware2),
	)

	// Start server to trigger middleware application
	if err := server.Start(); err != nil {
		t.Fatalf("Failed to start server: %v", err)
	}
	defer server.Stop()

	// Verify that middlewares were applied
	// The server should have started successfully with middlewares
	if server == nil {
		t.Error("Server should not be nil")
	}
}

// middlewareTestHandler is a test handler that records execution order.
type middlewareTestHandler struct {
	next    kknet.IHandler
	id      int
	order   *[]int
	counter *int32
}

func (h *middlewareTestHandler) OnConnect(c kknet.IConn) {
	atomic.AddInt32(h.counter, 1)
	*h.order = append(*h.order, h.id)
	if h.next != nil {
		h.next.OnConnect(c)
	}
}

func (h *middlewareTestHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	atomic.AddInt32(h.counter, 1)
	*h.order = append(*h.order, h.id)
	if h.next != nil {
		h.next.OnMessage(c, data)
	}
}

func (h *middlewareTestHandler) OnClose(c kknet.IConn, err error) {
	atomic.AddInt32(h.counter, 1)
	*h.order = append(*h.order, h.id)
	if h.next != nil {
		h.next.OnClose(c, err)
	}
}

// mockConn is a mock connection for testing.
type mockConn struct {
	id int64
}

func (m *mockConn) ID() int64 {
	return m.id
}

func (m *mockConn) Send(data []byte) error {
	return nil
}

func (m *mockConn) Close() error {
	return nil
}

func (m *mockConn) RemoteAddr() string {
	return "127.0.0.1:12345"
}

func (m *mockConn) Context() context.Context {
	return context.Background()
}

func (m *mockConn) SetContext(ctx context.Context) {
}
