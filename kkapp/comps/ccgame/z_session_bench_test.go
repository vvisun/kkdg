package ccgame

import (
	"fmt"
	"strconv"
	"testing"
)

func Benchmark_SessionManager_AddSession(b *testing.B) {
	mgr := newSessionManager()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sid := strconv.FormatInt(int64(i), 10)
		mgr.AddSession(sid, "gate1")
	}
}

func Benchmark_SessionManager_GetSession(b *testing.B) {
	mgr := newSessionManager()
	n := 10000
	for i := 0; i < n; i++ {
		sid := strconv.FormatInt(int64(i), 10)
		mgr.AddSession(sid, "gate1")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sid := strconv.FormatInt(int64(i%n), 10)
		_ = mgr.GetSession(sid)
	}
}

func Benchmark_SessionManager_RemoveSession(b *testing.B) {
	mgr := newSessionManager()
	for i := 0; i < b.N; i++ {
		sid := strconv.FormatInt(int64(i), 10)
		mgr.AddSession(sid, "gate1")
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sid := strconv.FormatInt(int64(i), 10)
		mgr.RemoveSession(sid)
	}
}

func Benchmark_SessionManager_AddGetRemove(b *testing.B) {
	mgr := newSessionManager()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		sid := strconv.FormatInt(int64(i), 10)
		mgr.AddSession(sid, "gate1")
		_ = mgr.GetSession(sid)
		mgr.RemoveSession(sid)
	}
}

func Benchmark_SessionManager_GetSession_Parallel(b *testing.B) {
	mgr := newSessionManager()
	n := 10000
	for i := 0; i < n; i++ {
		sid := strconv.FormatInt(int64(i), 10)
		mgr.AddSession(sid, "gate1")
	}
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sid := strconv.FormatInt(int64(i%n), 10)
			_ = mgr.GetSession(sid)
			i++
		}
	})
}

func Benchmark_SessionManager_AddGetRemove_Parallel(b *testing.B) {
	mgr := newSessionManager()
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sid := fmt.Sprintf("p%d_%d", i%64, i)
			mgr.AddSession(sid, "gate1")
			_ = mgr.GetSession(sid)
			mgr.RemoveSession(sid)
			i++
		}
	})
}
