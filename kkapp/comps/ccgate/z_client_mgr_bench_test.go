package ccgate

import (
	"fmt"
	"testing"

	"github.com/vvisun/kkdg/kknet"
)

func Benchmark_clientManager_addClient(b *testing.B) {
	m := newClientManager()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		connID := kknet.CONN_ID(i)
		m.addClient(connID, getSessionId(connID, "gate1"))
	}
}

func Benchmark_clientManager_getClient(b *testing.B) {
	m := newClientManager()
	n := 10000
	for i := 0; i < n; i++ {
		m.addClient(kknet.CONN_ID(i), getSessionId(kknet.CONN_ID(i), "gate1"))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = m.getClientByConnId(kknet.CONN_ID(i % n))
	}
}

func Benchmark_clientManager_removeClient(b *testing.B) {
	m := newClientManager()
	for i := 0; i < b.N; i++ {
		connID := kknet.CONN_ID(i)
		m.addClient(connID, getSessionId(connID, "gate1"))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.removeClient(kknet.CONN_ID(i))
	}
}

func Benchmark_clientManager_allocLogicNode(b *testing.B) {
	m := newClientManager()
	n := 1000
	for i := 0; i < n; i++ {
		m.addClient(kknet.CONN_ID(i), getSessionId(kknet.CONN_ID(i), "gate1"))
	}
	nodeTypes := []string{"game", "lobby", "chat"}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		connID := kknet.CONN_ID(i % n)
		nt := nodeTypes[i%len(nodeTypes)]
		_ = m.allocLogicNode(connID, nt, fmt.Sprintf("%s_%d", nt, i))
	}
}

func Benchmark_clientManager_AddGetRemove(b *testing.B) {
	m := newClientManager()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		connID := kknet.CONN_ID(i)
		sessionID := getSessionId(connID, "gate1")
		m.addClient(connID, sessionID)
		_ = m.getClientByConnId(connID)
		m.removeClient(connID)
	}
}

func Benchmark_clientManager_GetClient_Parallel(b *testing.B) {
	m := newClientManager()
	n := 10000
	for i := 0; i < n; i++ {
		m.addClient(kknet.CONN_ID(i), getSessionId(kknet.CONN_ID(i), "gate1"))
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = m.getClientByConnId(kknet.CONN_ID(i % n))
			i++
		}
	})
}

func Benchmark_clientManager_AddGetRemove_Parallel(b *testing.B) {
	m := newClientManager()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			connID := kknet.CONN_ID(i)
			sessionID := getSessionId(connID, "gate1")
			m.addClient(connID, sessionID)
			_ = m.getClientByConnId(connID)
			m.removeClient(connID)
			i++
		}
	})
}

func Benchmark_getSessionId(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = getSessionId(kknet.CONN_ID(i), "gate1")
	}
}
