package kknet

import (
	"context"
	"sync/atomic"
	"testing"
)

type testConn struct {
	id     int64
	closed atomic.Bool
}

func (c *testConn) ID() int64 {
	return c.id
}

func (c *testConn) Send(_ []byte) error {
	return nil
}

func (c *testConn) Close() error {
	c.closed.Store(true)
	return nil
}

func (c *testConn) RemoteAddr() string {
	return "127.0.0.1:0"
}

func (c *testConn) Context() context.Context {
	return context.Background()
}

func (c *testConn) SetContext(_ context.Context) {}

func TestConnManagerBasic(t *testing.T) {
	mgr := NewConnManager()

	c1 := &testConn{id: 1}
	c2 := &testConn{id: 2}

	mgr.AddConn(c1)
	mgr.AddConn(c2)

	if got := mgr.GetConn(1); got != c1 {
		t.Fatalf("expected conn 1, got %v", got)
	}
	if got := mgr.GetConn(2); got != c2 {
		t.Fatalf("expected conn 2, got %v", got)
	}

	all := mgr.GetAllConns()
	if len(all) != 2 {
		t.Fatalf("expected 2 conns, got %d", len(all))
	}

	mgr.KickConn(1)
	if mgr.GetConn(1) != nil {
		t.Fatalf("expected conn 1 removed")
	}
	if !c1.closed.Load() {
		t.Fatalf("expected conn 1 closed")
	}

	mgr.RemoveConn(2)
	if mgr.GetConn(2) != nil {
		t.Fatalf("expected conn 2 removed")
	}
}
