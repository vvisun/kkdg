package gametrans

import (
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/vvisun/kkdg/kkapp/transport"
)

func TestSessionManager_AddGetRemove(t *testing.T) {
	mgr := NewSessionManager(4)

	if mgr.OnlineCount() != 0 {
		t.Fatalf("initial OnlineCount = %d, want 0", mgr.OnlineCount())
	}
	if got := mgr.GetSession("s1"); got != nil {
		t.Fatalf("GetSession empty = %v, want nil", got)
	}

	mgr.AddSession("s1", "gate1")
	si := mgr.GetSession("s1")
	if si == nil {
		t.Fatal("GetSession(s1) = nil after AddSession")
	}
	if si.GetSessionID() != "s1" || si.GetGateNodeID() != "gate1" {
		t.Errorf("GetSession(s1) = SessionID=%q GateNodeID=%q, want s1, gate1", si.GetSessionID(), si.GetGateNodeID())
	}
	if w := mgr.GetWorkersCount(); si.GetThreadIdx() < 0 || si.GetThreadIdx() >= w {
		t.Errorf("threadIdx = %d, want in [0, %d)", si.GetThreadIdx(), w)
	}
	if mgr.OnlineCount() != 1 {
		t.Fatalf("after AddSession: OnlineCount = %d, want 1", mgr.OnlineCount())
	}

	mgr.RemoveSession("s1")
	if got := mgr.GetSession("s1"); got != nil {
		t.Fatalf("GetSession(s1) after Remove = %v, want nil", got)
	}
	if mgr.OnlineCount() != 0 {
		t.Fatalf("after RemoveSession: OnlineCount = %d, want 0", mgr.OnlineCount())
	}
}

func TestSessionManager_AddSession_Idempotent(t *testing.T) {
	mgr := NewSessionManager(4)

	mgr.AddSession("s1", "gate1")
	mgr.AddSession("s1", "gate2")
	si := mgr.GetSession("s1")
	if si == nil {
		t.Fatal("GetSession(s1) = nil")
	}
	if si.GetGateNodeID() != "gate1" {
		t.Errorf("idempotent AddSession: GateNodeID = %q, want gate1", si.GetGateNodeID())
	}
	if mgr.OnlineCount() != 1 {
		t.Fatalf("OnlineCount = %d, want 1 (second add must not duplicate)", mgr.OnlineCount())
	}
}

func TestSessionManager_NewSessionManager_WorkersCountDefaults(t *testing.T) {
	for _, w := range []int{0, -1, -100} {
		mgr := NewSessionManager(w)
		if mgr.GetWorkersCount() != 1 {
			t.Errorf("NewSessionManager(%d).GetWorkersCount() = %d, want 1", w, mgr.GetWorkersCount())
		}
	}
}

func TestSessionIdToThreadIdx_RangeAndStable(t *testing.T) {
	const workers = 7
	for _, sid := range []string{"", "a", "gate-1-42", "很长-session-标识-测试"} {
		idx := sessionIdToThreadIdx(sid, workers)
		if idx < 0 || idx >= workers {
			t.Errorf("sessionIdToThreadIdx(%q, %d) = %d, want in [0, %d)", sid, workers, idx, workers)
		}
	}
	a := sessionIdToThreadIdx("same", workers)
	b := sessionIdToThreadIdx("same", workers)
	if a != b {
		t.Errorf("sessionIdToThreadIdx not stable: %d vs %d", a, b)
	}
}

func TestSessionManager_AddSessionWithShard_Explicit(t *testing.T) {
	mgr := NewSessionManager(4)
	const wantShard = 3
	si := mgr.AddSessionWithShard("s1", "gate1", wantShard)
	if si == nil {
		t.Fatal("AddSessionWithShard returned nil")
	}
	if si.GetShardIdx() != wantShard {
		t.Errorf("ShardIdx = %d, want %d", si.GetShardIdx(), wantShard)
	}
}

func TestSessionManager_AddSessionWithShard_AutoShardWhenInvalid(t *testing.T) {
	mgr := NewSessionManager(4)
	for _, shard := range []int{-1, transport.BackendShardCnt, transport.BackendShardCnt + 10} {
		sid := "auto-" + strconv.Itoa(shard)
		si := mgr.AddSessionWithShard(sid, "g1", shard)
		if si == nil {
			t.Fatalf("AddSessionWithShard(%q) nil", sid)
		}
		s := si.GetShardIdx()
		if s < 0 || s >= transport.BackendShardCnt {
			t.Errorf("auto shard for %q: got %d, want in [0, %d)", sid, s, transport.BackendShardCnt)
		}
	}
}

func TestSessionManager_PutSessionInfo_Duplicate(t *testing.T) {
	si := newSessionInfo()
	si.sessionID = "s1"
	si.gateNodeID = "gate1"
	if si.isInPool.Load() {
		t.Fatal("SessionInfo should not start in pool")
	}
	putSessionInfo(si)
	if !si.isInPool.Load() {
		t.Fatal("SessionInfo should be marked in pool after put")
	}
	putSessionInfo(si)
}

func TestSessionManager_ConcurrentAddRemove_DisjointSessions(t *testing.T) {
	mgr := NewSessionManager(8)

	const goroutines = 8
	const perG = 256

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer wg.Done()
			base := g * perG
			for i := 0; i < perG; i++ {
				id := base + i
				sessionID := fmt.Sprintf("s-%d", id)
				gateID := fmt.Sprintf("gate-%d", g)
				mgr.AddSession(sessionID, gateID)
				mgr.RemoveSession(sessionID)
			}
		}()
	}

	wg.Wait()

	if got := mgr.OnlineCount(); got != 0 {
		t.Fatalf("after concurrent add/remove: OnlineCount = %d, want 0", got)
	}
}

// 并发仅 Add：不同 sessionID，最终 OnlineCount 应等于成功添加数（每 ID 一次）。
func TestSessionManager_ConcurrentAddOnly_DisjointSessions(t *testing.T) {
	mgr := NewSessionManager(4)
	const n = 2000

	var wg sync.WaitGroup
	var fail atomic.Int32
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			defer wg.Done()
			sid := strconv.Itoa(i)
			mgr.AddSession(sid, "gate")
			if mgr.GetSession(sid) == nil {
				fail.Add(1)
			}
		}()
	}
	wg.Wait()

	if fail.Load() != 0 {
		t.Fatalf("GetSession nil after Add for %d goroutines", fail.Load())
	}
	if got := mgr.OnlineCount(); got != n {
		t.Fatalf("OnlineCount = %d, want %d", got, n)
	}
}
