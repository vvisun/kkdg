package ccgate

import (
	"strconv"
	"testing"
)

// Test basic bind / unbind / count behavior.
func Test_localDidcovery_basic(t *testing.T) {
	m := newLocalDiscovery()

	const nodeID = "node-1"
	const nodeTypeA = "game"
	const nodeTypeB = "chat"

	// 未绑定前，count 应为 0
	if c := m.getSessionCount(nodeID); c != 0 {
		t.Fatalf("getSessionCount(%q) before bind = %d, want 0", nodeID, c)
	}

	// 绑定多个 session
	m.onBindLogicNode("s1", nodeTypeA, nodeID)
	m.onBindLogicNode("s2", nodeTypeA, nodeID)
	m.onBindLogicNode("s3", nodeTypeB, nodeID)

	if c := m.getSessionCount(nodeID); c != 3 {
		t.Fatalf("getSessionCount(%q) after bind = %d, want 3", nodeID, c)
	}

	// 解绑一个 session
	m.onUnbindLogicNode("s2", nodeTypeA, nodeID)
	if c := m.getSessionCount(nodeID); c != 2 {
		t.Fatalf("getSessionCount(%q) after unbind = %d, want 2", nodeID, c)
	}

	// 解绑不存在的 session 不应 panic，数量不变
	m.onUnbindLogicNode("s-not-exist", nodeTypeA, nodeID)
	if c := m.getSessionCount(nodeID); c != 2 {
		t.Fatalf("getSessionCount(%q) after unbind non-exist = %d, want 2", nodeID, c)
	}

	// 查询不存在的节点，count 应为 0
	if c := m.getSessionCount("unknown-node"); c != 0 {
		t.Fatalf("getSessionCount(%q) = %d, want 0", "unknown-node", c)
	}
}

// Benchmark concurrent bind / unbind on a single node.
func Benchmark_localDidcovery_bind_unbind(b *testing.B) {
	m := newLocalDiscovery()
	const nodeID = "node-bench"

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sessionID := "s-" + strconv.Itoa(i)
			m.onBindLogicNode(sessionID, "bench", nodeID)
			m.onUnbindLogicNode(sessionID, "bench", nodeID)
			i++
		}
	})
}
