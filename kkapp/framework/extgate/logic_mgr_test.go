package extgate

import (
	"net"
	"testing"

	"github.com/vvisun/kkdg/kkapp/framework/extmsg"
	"github.com/vvisun/kkdg/kknet"
)

func TestLogicServerMgr_addGetRemoveLogicServer(t *testing.T) {
	m := &LogicServerMgr{}

	info := &extmsg.RegisterMsg{NodeId: "node1", NodeType: "logic", ShardIdx: 0}
	m.addLogicServer(info)
	ls := m.getLogicServer("node1")
	if ls == nil {
		t.Fatal("getLogicServer want non-nil after addLogicServer")
	}
	if ls.nodeId != "node1" || ls.nodeType != "logic" {
		t.Errorf("getLogicServer: nodeId=%s nodeType=%s", ls.nodeId, ls.nodeType)
	}

	// 重复添加不覆盖
	m.addLogicServer(info)
	ls2 := m.getLogicServer("node1")
	if ls2 != ls {
		t.Error("addLogicServer twice should keep same instance")
	}

	m.removeLogicServer("node1")
	if m.getLogicServer("node1") != nil {
		t.Error("getLogicServer want nil after removeLogicServer")
	}
}

func TestLogicServerMgr_addGetRemoveShardConn(t *testing.T) {
	m := &LogicServerMgr{}
	info := &extmsg.RegisterMsg{NodeId: "n1", NodeType: "logic", ShardIdx: 0}
	m.addLogicServer(info)

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	connID := kknet.NextConnID()
	sc := &ShardConn{conn: c1, connId: connID, shardIdx: -1, nodeId: ""}
	m.addShardConn("n1", 0, sc)

	got := m.getShardConn("n1", 0)
	if got != sc {
		t.Fatalf("getShardConn(0) = %p, want %p", got, sc)
	}
	if got.shardIdx != 0 || got.nodeId != "n1" {
		t.Errorf("shardConn: shardIdx=%d nodeId=%s", got.shardIdx, got.nodeId)
	}

	m.removeShardConn("n1", 0)
	if m.getShardConn("n1", 0) != nil {
		t.Error("getShardConn(0) want nil after removeShardConn")
	}
}

func TestLogicServerMgr_removeShardConn_invalidShardIdx(t *testing.T) {
	m := &LogicServerMgr{}
	info := &extmsg.RegisterMsg{NodeId: "n1", NodeType: "logic", ShardIdx: 0}
	m.addLogicServer(info)

	// 不应 panic
	m.removeShardConn("n1", -1)
	m.removeShardConn("n1", BackendShardCnt)
	m.removeShardConn("n1", 99)
}

func TestLogicServerMgr_addShardConn_invalidShardIdx(t *testing.T) {
	m := &LogicServerMgr{}
	info := &extmsg.RegisterMsg{NodeId: "n1", NodeType: "logic", ShardIdx: 0}
	m.addLogicServer(info)

	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	sc := &ShardConn{conn: c1, connId: kknet.NextConnID(), shardIdx: -1}

	m.addShardConn("n1", -1, sc)
	if m.getShardConn("n1", 0) != nil {
		t.Error("addShardConn(-1) should not add")
	}
	m.addShardConn("n1", BackendShardCnt, sc)
	// 未添加到有效槽位
	m.addShardConn("n1", 0, sc)
	if m.getShardConn("n1", 0) != sc {
		t.Error("addShardConn(0) should add")
	}
}

func TestLogicServerMgr_addShardConn_unknownNodeId(t *testing.T) {
	m := &LogicServerMgr{}
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()
	sc := &ShardConn{conn: c1, connId: kknet.NextConnID()}
	// 未注册的 nodeId，不应 panic
	m.addShardConn("unknown", 0, sc)
}

func TestLogicServerMgr_multipleShards(t *testing.T) {
	m := &LogicServerMgr{}
	info := &extmsg.RegisterMsg{NodeId: "n1", NodeType: "logic", ShardIdx: 0}
	m.addLogicServer(info)

	for i := 0; i < BackendShardCnt; i++ {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()
		sc := &ShardConn{conn: c1, connId: kknet.NextConnID(), shardIdx: -1}
		m.addShardConn("n1", i, sc)
	}

	for i := 0; i < BackendShardCnt; i++ {
		sc := m.getShardConn("n1", i)
		if sc == nil {
			t.Errorf("getShardConn(n1, %d) = nil", i)
		}
		if sc.shardIdx != i {
			t.Errorf("shardIdx = %d, want %d", sc.shardIdx, i)
		}
	}

	for i := 0; i < BackendShardCnt; i++ {
		m.removeShardConn("n1", i)
		for j := 0; j < BackendShardCnt; j++ {
			got := m.getShardConn("n1", j)
			if j <= i && got != nil {
				t.Errorf("after remove %d, getShardConn(n1,%d) should be nil", i, j)
			}
			if j > i && got == nil {
				t.Errorf("after remove %d, getShardConn(n1,%d) should be non-nil", i, j)
			}
		}
	}
}
