package ccgate

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet"
)

func TestFeedLimit_IsLimited_BasicWindow(t *testing.T) {
	f := NewFeedLimit(time.Second)
	const connID kknet.CONN_ID = 1
	const code GateErrorCode = ERR_CLIENT_INVALID_PACKET

	// first call should not be limited
	if f.IsLimited(connID, code) {
		t.Fatalf("first IsLimited call should return false")
	}

	// immediate second call should be limited
	if !f.IsLimited(connID, code) {
		t.Fatalf("second IsLimited call within window should return true")
	}
}

func TestFeedLimit_IsLimited_ExpiredWindow(t *testing.T) {
	f := NewFeedLimit(time.Second)
	const connID kknet.CONN_ID = 2
	const code GateErrorCode = ERR_RECV_QUEUE_FULL

	if f.IsLimited(connID, code) {
		t.Fatalf("first IsLimited call should return false")
	}
	if !f.IsLimited(connID, code) {
		t.Fatalf("second IsLimited call within window should return true")
	}

	// wait for window to expire
	time.Sleep(1100 * time.Millisecond)

	// after window, should allow again
	if f.IsLimited(connID, code) {
		t.Fatalf("IsLimited should return false after window expires")
	}
}

func TestFeedLimit_IsLimited_DifferentConnOrCode(t *testing.T) {
	f := NewFeedLimit(time.Second)
	const connID1 kknet.CONN_ID = 3
	const connID2 kknet.CONN_ID = 4

	if f.IsLimited(connID1, ERR_ALLOC_LOGIC_NODE_FAILED) {
		t.Fatalf("first call for conn1/code1 should be false")
	}
	if f.IsLimited(connID1, ERR_USER_KICKED) {
		t.Fatalf("first call for conn1/code2 should be false")
	}
	if f.IsLimited(connID2, ERR_ALLOC_LOGIC_NODE_FAILED) {
		t.Fatalf("first call for conn2/code1 should be false")
	}
}

func TestFeedLimit_Reset(t *testing.T) {
	f := NewFeedLimit(time.Second)
	const connID kknet.CONN_ID = 5
	const code GateErrorCode = ERR_CLIENT_INVALID_PACKET

	if f.IsLimited(connID, code) {
		t.Fatalf("first IsLimited call should return false")
	}
	if !f.IsLimited(connID, code) {
		t.Fatalf("second IsLimited call within window should return true")
	}

	// reset should open a fresh window; immediate call after reset should be limited,
	// but after sleep it should allow again
	f.Reset(connID, code)

	if !f.IsLimited(connID, code) {
		t.Fatalf("call immediately after Reset still within window should return true")
	}

	time.Sleep(1100 * time.Millisecond)
	if f.IsLimited(connID, code) {
		t.Fatalf("after reset window expires, IsLimited should return false")
	}
}

// Benchmark single-threaded IsLimited calls on same (connID, errCode).
func BenchmarkFeedLimit_IsLimited_SameKey(b *testing.B) {
	f := NewFeedLimit(time.Second)
	const connID kknet.CONN_ID = 100
	const code GateErrorCode = ERR_CLIENT_INVALID_PACKET

	for i := 0; i < b.N; i++ {
		f.IsLimited(connID, code)
	}
}

// Benchmark IsLimited with varying connID to simulate many connections.
func BenchmarkFeedLimit_IsLimited_ManyConns(b *testing.B) {
	f := NewFeedLimit(time.Second)
	const code GateErrorCode = ERR_RECV_QUEUE_FULL

	for i := 0; i < b.N; i++ {
		f.IsLimited(kknet.CONN_ID(i), code)
	}
}

// Benchmark concurrent IsLimited calls.
func BenchmarkFeedLimit_IsLimited_Parallel(b *testing.B) {
	f := NewFeedLimit(time.Second)
	const code GateErrorCode = ERR_ALLOC_LOGIC_NODE_FAILED

	b.RunParallel(func(pb *testing.PB) {
		var id kknet.CONN_ID = 0
		for pb.Next() {
			f.IsLimited(id, code)
			id++
		}
	})
}
