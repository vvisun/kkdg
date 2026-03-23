package gatetrans

import (
	"strconv"
	"sync"
	"testing"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// mockConn is a lightweight fake implementation for benchmarking.
type mockConn struct {
	id kknet.CONN_ID
}

func (m *mockConn) ID() kknet.CONN_ID                            { return m.id }
func (m *mockConn) Close() error                                 { return nil }
func (m *mockConn) RemoteAddr() string                           { return "" }
func (m *mockConn) LocalAddr() string                            { return "" }
func (m *mockConn) GetConnManager() kknet.IConnManager           { return nil }
func (m *mockConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error { return nil }
func (m *mockConn) SendMsg(msg any) error                        { return nil }

// helper to build many session ids without extra allocations in the hot loop.
func makeSessionIDs(n int) []string {
	ids := make([]string, n)
	for i := 0; i < n; i++ {
		ids[i] = "sess-" + strconv.Itoa(i)
	}
	return ids
}

// BenchmarkSessionManager_AddConn measures pure write throughput.
func BenchmarkSessionManager_AddConn(b *testing.B) {
	const sessionCount = 100
	ids := makeSessionIDs(sessionCount)
	mgr := NewSessionMgr()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < sessionCount; j++ {
			mgr.AddConn(ids[j], &mockConn{id: kknet.CONN_ID(j)})
		}
	}
}

func BenchmarkConnManager_AddConn(b *testing.B) {
	const sessionCount = 100
	mgr := kknet.NewConnManager[*mockConn]()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < sessionCount; j++ {
			mgr.AddConn(&mockConn{id: kknet.CONN_ID(j)})
		}
	}
}

// BenchmarkSessionManager_GetConnHit measures GetConn 命中场景性能。
func BenchmarkSessionManager_GetConnHit(b *testing.B) {
	const sessionCount = 10000
	ids := makeSessionIDs(sessionCount)
	mgr := NewSessionMgr()
	for i := 0; i < sessionCount; i++ {
		mgr.AddConn(ids[i], &mockConn{id: kknet.CONN_ID(i)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := ids[i%sessionCount]
		_, _ = mgr.GetConn(id)
	}
}

func BenchmarkConnManager_GetConn(b *testing.B) {
	const sessionCount = 10000
	mgr := kknet.NewConnManager[*mockConn]()
	for i := 0; i < sessionCount; i++ {
		mgr.AddConn(&mockConn{id: kknet.CONN_ID(i)})
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := kknet.CONN_ID(i % sessionCount)
		_ = mgr.GetConn(id)
	}
}

// BenchmarkSessionManager_GetConnMiss measures GetConn 未命中场景性能。
func BenchmarkSessionManager_GetConnMiss(b *testing.B) {
	mgr := NewSessionMgr()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = mgr.GetConn("unknown-session-id")
	}
}

func BenchmarkConnManager_GetConnMiss(b *testing.B) {
	mgr := kknet.NewConnManager[*mockConn]()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mgr.GetConn(kknet.CONN_ID(i))
	}
}

// BenchmarkSessionManager_ConcurrentAddGet 模拟网关高并发场景：
// 不断 AddConn / GetConn / RemoveConn。
func BenchmarkSessionManager_ConcurrentAddGet(b *testing.B) {
	const sessionCount = 10000
	ids := makeSessionIDs(sessionCount)
	mgr := NewSessionMgr()

	var wg sync.WaitGroup
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		wg.Add(3)

		// writer: AddConn
		go func() {
			defer wg.Done()
			for j := 0; j < sessionCount; j++ {
				mgr.AddConn(ids[j], &mockConn{id: kknet.CONN_ID(j)})
			}
		}()

		// reader: GetConn
		go func() {
			defer wg.Done()
			for j := 0; j < sessionCount; j++ {
				id := ids[j%sessionCount]
				_, _ = mgr.GetConn(id)
			}
		}()

		// remover: RemoveConn
		go func() {
			defer wg.Done()
			for j := 0; j < sessionCount; j++ {
				mgr.RemoveConn(ids[j])
			}
		}()

		wg.Wait()
	}
}

func BenchmarkConnManager_ConcurrentAddGet(b *testing.B) {
	const sessionCount = 10000
	mgr := kknet.NewConnManager[*mockConn]()

	var wg sync.WaitGroup
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(3)

		// writer: AddConn
		go func() {
			defer wg.Done()
			for j := 0; j < sessionCount; j++ {
				mgr.AddConn(&mockConn{id: kknet.CONN_ID(j)})
			}
		}()

		// reader: GetConn
		go func() {
			defer wg.Done()
			for j := 0; j < sessionCount; j++ {
				id := kknet.CONN_ID(j % sessionCount)
				_ = mgr.GetConn(id)
			}
		}()

		// remover: RemoveConn
		go func() {
			defer wg.Done()
			for j := 0; j < sessionCount; j++ {
				mgr.RemoveConn(kknet.CONN_ID(j))
			}
		}()
	}
	wg.Wait()
}
