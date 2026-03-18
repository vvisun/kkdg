package dnats

import (
	"sync/atomic"
	"testing"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
)

// newTestDiscovery creates a NatsDiscovery without connecting to a real NATS server.
func newTestDiscovery(t *testing.T) *NatsDiscovery {
	t.Helper()
	nodeInfo := kkapp.NewNodeInfo("node1", kkapp.NodeTypeGate, "127.0.0.1:8000", "")
	d := NewNatsDiscovery("test", nodeInfo, ApplyNatsOptions(), kkdiscovery.ApplyOptions())
	nd, ok := d.(*NatsDiscovery)
	if !ok || nd == nil {
		t.Fatalf("NewNatsDiscovery returned %T, want *NatsDiscovery", d)
	}
	return nd
}

// TestNatsDiscovery_AddRemove_StatsAndDelegation verifies that NatsDiscovery delegates
// member operations to MemberMgr and updates stats consistently.
func TestNatsDiscovery_AddRemove_StatsAndDelegation(t *testing.T) {
	d := newTestDiscovery(t)

	var addCount, removeCount atomic.Int64
	d.GetMemberObserver().ObserveAddMember(func(m kkdiscovery.IMember) {
		addCount.Add(1)
	})
	d.GetMemberObserver().ObserveRemoveMember(func(m kkdiscovery.IMember) {
		removeCount.Add(1)
	})

	info := &kkdiscovery.MemberInfo{
		NodeID:   "node2",
		NodeType: "logic",
		Address:  "127.0.0.1:9000",
		Weight:   1,
		Status:   kkdiscovery.NodeStatusOnline,
	}

	// Add member via internal API and verify state.
	d.addMemberInfo(info)

	if got := d.GetMemberMgr().MemberCount(); got != 1 {
		t.Fatalf("MemberCount after add = %d, want 1", got)
	}
	m, ok := d.GetMemberMgr().GetMember("node2")
	if !ok || m == nil {
		t.Fatalf("GetMember(node2) = (%v,%v), want non-nil,true", m, ok)
	}
	if typ, err := d.GetMemberMgr().GetNodeType("node2"); err != nil || typ != "logic" {
		t.Fatalf("GetType(node2) = (%q,%v), want (\"logic\",nil)", typ, err)
	}

	// RangeType should see this member.
	d.GetMemberMgr().RangeType("logic", func(nodeID string, member kkdiscovery.IMember) bool {
		if nodeID != "node2" || member.GetNodeID() != "node2" {
			t.Fatalf("RangeType(logic) = (%v,%v), want (node2,node2)", nodeID, member.GetNodeID())
		}
		return false
	})

	// Stats should reflect 1 member added.
	snap := d.Stats()
	if snap.MemberCount != 1 || snap.MembersAdded != 1 || snap.MembersRemoved != 0 {
		t.Fatalf("Stats after add = %+v, want MemberCount=1 MembersAdded=1 MembersRemoved=0", snap)
	}
	if addCount.Load() != 1 {
		t.Fatalf("OnAddMember called %d times, want 1", addCount.Load())
	}

	// Remove the member and verify everything is cleaned up.
	d.removeMember("node2")

	if got := d.GetMemberMgr().MemberCount(); got != 0 {
		t.Fatalf("MemberCount after remove = %d, want 0", got)
	}
	if m, ok := d.GetMemberMgr().GetMember("node2"); ok || m != nil {
		t.Fatalf("GetMember(node2) after remove = (%v,%v), want (nil,false)", m, ok)
	}
	snap = d.Stats()
	if snap.MemberCount != 0 || snap.MembersAdded != 1 || snap.MembersRemoved != 1 {
		t.Fatalf("Stats after remove = %+v, want MemberCount=0 MembersAdded=1 MembersRemoved=1", snap)
	}
	if removeCount.Load() != 1 {
		t.Fatalf("OnRemoveMember called %d times, want 1", removeCount.Load())
	}
}
