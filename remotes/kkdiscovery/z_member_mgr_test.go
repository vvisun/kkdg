package kkdiscovery

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/vvisun/kkdg/kkerrors"
)

func newTestMemberInfo(id, tpe, addr string, weight, status int) *MemberInfo {
	return &MemberInfo{
		NodeID:   id,
		NodeType: tpe,
		Address:  addr,
		Weight:   weight,
		Status:   status,
		Settings: map[string]string{"k": "v"},
	}
}

func TestMemberMgr_AddGetRemove(t *testing.T) {
	mgr := NewMemberMgr()
	if mgr.MemberCount() != 0 {
		t.Fatalf("initial MemberCount = %d, want 0", mgr.MemberCount())
	}

	info := newTestMemberInfo("n1", "logic", "127.0.0.1:1", 10, NodeStatusOnline)
	member := mgr.AddMember(info)
	if member == nil {
		t.Fatal("AddMember returned nil")
	}
	if got := mgr.MemberCount(); got != 1 {
		t.Fatalf("MemberCount after AddMember = %d, want 1", got)
	}

	got, ok := mgr.GetMember("n1")
	if !ok || got == nil {
		t.Fatal("GetMember(n1) = nil, false, want member, true")
	}
	if got.GetNodeID() != "n1" || got.GetNodeType() != "logic" || got.GetAddress() != "127.0.0.1:1" {
		t.Fatalf("GetMember(n1) unexpected fields: id=%s type=%s addr=%s",
			got.GetNodeID(), got.GetNodeType(), got.GetAddress())
	}

	// update existing member
	info2 := newTestMemberInfo("n1", "logic2", "127.0.0.1:2", 20, NodeStatusOffline)
	_ = mgr.AddMember(info2)
	got2, ok := mgr.GetMember("n1")
	if !ok || got2 == nil {
		t.Fatal("GetMember(n1) after update = nil, false")
	}
	if got2.GetNodeType() != "logic2" || got2.GetAddress() != "127.0.0.1:2" || got2.GetWeight() != 20 || got2.GetStatus() != NodeStatusOffline {
		t.Fatalf("updated member mismatch: type=%s addr=%s weight=%d status=%d",
			got2.GetNodeType(), got2.GetAddress(), got2.GetWeight(), got2.GetStatus())
	}

	// remove
	mgr.RemoveMember("n1")
	if mgr.MemberCount() != 0 {
		t.Fatalf("MemberCount after RemoveMember = %d, want 0", mgr.MemberCount())
	}
	if m, ok := mgr.GetMember("n1"); ok || m != nil {
		t.Fatalf("GetMember(n1) after RemoveMember = (%v,%v), want (nil,false)", m, ok)
	}
}

func idsOf(list []IMember) []string {
	out := make([]string, 0, len(list))
	for _, m := range list {
		out = append(out, m.GetNodeID())
	}
	return out
}

func TestMemberMgr_GetType(t *testing.T) {
	mgr := NewMemberMgr()
	mgr.AddMember(newTestMemberInfo("n1", "logic", "addr1", 1, NodeStatusOnline))

	typ, err := mgr.GetType("n1")
	if err != nil {
		t.Fatalf("GetType(n1) error = %v", err)
	}
	if typ != "logic" {
		t.Fatalf("GetType(n1) = %q, want \"logic\"", typ)
	}

	if _, err := mgr.GetType("missing"); err != kkerrors.ErrClusterMemberNotFound {
		t.Fatalf("GetType(missing) error = %v, want ErrMemberNotFound", err)
	}
}

func TestMemberMgr_ListenersAndPanicSafety(t *testing.T) {
	mgr := NewMemberMgr()

	var addCount, removeCount atomic.Int64

	mgr.ObserveAddMember(func(m IMember) {
		addCount.Add(1)
	})
	mgr.ObserveRemoveMember(func(m IMember) {
		removeCount.Add(1)
	})

	// 注册一个会 panic 的监听器，验证不会影响其他监听器的执行
	mgr.ObserveAddMember(func(IMember) {
		panic("add panic")
	})
	mgr.ObserveRemoveMember(func(IMember) {
		panic("remove panic")
	})

	info := newTestMemberInfo("n1", "logic", "addr1", 1, NodeStatusOnline)
	mgr.AddMember(info)
	mgr.RemoveMember("n1")

	if addCount.Load() != 1 {
		t.Fatalf("addCount = %d, want 1", addCount.Load())
	}
	if removeCount.Load() != 1 {
		t.Fatalf("removeCount = %d, want 1", removeCount.Load())
	}
}

func TestMemberMgr_Range(t *testing.T) {
	mgr := NewMemberMgr()
	mgr.AddMember(newTestMemberInfo("n1", "logic", "addr1", 1, NodeStatusOnline))
	mgr.AddMember(newTestMemberInfo("n2", "logic", "addr2", 2, NodeStatusOnline))
	mgr.AddMember(newTestMemberInfo("n3", "gate", "addr3", 3, NodeStatusOnline))

	var seen []string
	mgr.Range(func(nodeID string, _ IMember) bool {
		seen = append(seen, nodeID)
		return true
	})
	if len(seen) != 3 {
		t.Fatalf("Range visited %d members, want 3", len(seen))
	}

	// 断言 Range 中返回 false 会提前终止
	seen = seen[:0]
	mgr.Range(func(nodeID string, _ IMember) bool {
		seen = append(seen, nodeID)
		return false
	})
	if len(seen) != 1 {
		t.Fatalf("Range with early stop visited %d members, want 1", len(seen))
	}
}

// --- Benchmarks ---

// BenchmarkMemberMgr_AddMember benchmarks AddMember with mixed new & existing members.
func BenchmarkMemberMgr_AddMember(b *testing.B) {
	mgr := NewMemberMgr()
	const base = 100000

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		id := fmt.Sprintf("node-%d", base+i%1024) // 控制总成员数量，模拟热点更新
		info := newTestMemberInfo(id, "logic", "addr", 1, NodeStatusOnline)
		_ = mgr.AddMember(info)
	}
}

// BenchmarkMemberMgr_Random benchmarks Random selection from a given type.
func BenchmarkMemberMgr_Random(b *testing.B) {
	mgr := NewMemberMgr()
	const total = 2048
	for i := 0; i < total; i++ {
		mgr.AddMember(newTestMemberInfo(fmt.Sprintf("n-%d", i), "logic", "addr", 1, NodeStatusOnline))
	}

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		m, ok := mgr.random("logic")
		if !ok || m == nil {
			b.Fatalf("Random(logic) returned nil, false")
		}
	}
}
