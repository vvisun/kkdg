package ccgate

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/vvisun/kkdg/kknet"
)

func Benchmark_clientManager_addClient(b *testing.B) {
	m := &clientManager{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		connID := kknet.CONN_ID(i)
		sessionID := strconv.FormatUint(uint64(i), 10)
		m.addClient(connID, sessionID)
	}
}

func Benchmark_clientManager_getClient(b *testing.B) {
	m := &clientManager{}
	n := 10000
	for i := 0; i < n; i++ {
		m.addClient(kknet.CONN_ID(i), strconv.FormatUint(uint64(i), 10))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = m.getClient(kknet.CONN_ID(i % n))
	}
}

func Benchmark_clientManager_removeClient(b *testing.B) {
	m := &clientManager{}
	for i := 0; i < b.N; i++ {
		connID := kknet.CONN_ID(i)
		m.addClient(connID, strconv.FormatUint(uint64(i), 10))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		m.removeClient(kknet.CONN_ID(i))
	}
}

func Benchmark_clientManager_loginToGate_getClientByUserId(b *testing.B) {
	m := &clientManager{}
	n := 10000
	for i := 0; i < n; i++ {
		connID := kknet.CONN_ID(i)
		m.addClient(connID, strconv.FormatUint(uint64(i), 10))
		m.loginToGate(connID, kknet.USER_ID(i+1))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = m.getClientByUserId(kknet.USER_ID((i % n) + 1))
	}
}

func Benchmark_clientManager_allocLogicNode(b *testing.B) {
	m := &clientManager{}
	n := 1000
	for i := 0; i < n; i++ {
		m.addClient(kknet.CONN_ID(i), strconv.FormatUint(uint64(i), 10))
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
	m := &clientManager{}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		connID := kknet.CONN_ID(i)
		sessionID := strconv.FormatUint(uint64(i), 10)
		m.addClient(connID, sessionID)
		_ = m.getClient(connID)
		m.removeClient(connID)
	}
}

func Benchmark_clientManager_GetClient_Parallel(b *testing.B) {
	m := &clientManager{}
	n := 10000
	for i := 0; i < n; i++ {
		m.addClient(kknet.CONN_ID(i), strconv.FormatUint(uint64(i), 10))
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = m.getClient(kknet.CONN_ID(i % n))
			i++
		}
	})
}

func Benchmark_clientManager_AddGetRemove_Parallel(b *testing.B) {
	m := &clientManager{}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			connID := kknet.CONN_ID(i)
			sessionID := fmt.Sprintf("p%d_%d", i%64, i)
			m.addClient(connID, sessionID)
			_ = m.getClient(connID)
			m.removeClient(connID)
			i++
		}
	})
}

func Benchmark_getSessionId(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = getSessionId(kknet.CONN_ID(i))
	}
}
