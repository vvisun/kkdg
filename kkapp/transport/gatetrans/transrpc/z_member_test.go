package transrpc

import (
	"testing"

	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

type stubConn struct {
	id kknet.CONN_ID
}

func (c *stubConn) ID() kknet.CONN_ID                            { return c.id }
func (c *stubConn) Close() error                                 { return nil }
func (c *stubConn) RemoteAddr() string                           { return "" }
func (c *stubConn) SendBuffer(buffer *kkbuffer.ByteBuffer) error { return nil }
func (c *stubConn) SendMsg(msg any) error                        { return nil }

func countMembers(mgr *logicNodeMgr) int {
	n := 0
	mgr.Range(func(string, gatetrans.IMember) bool {
		n++
		return true
	})
	return n
}

func TestOnClose_UnregistersLogicNode(t *testing.T) {
	mgr := newLogicNodeMgr()
	trans := &transportorRpc{logicNodeMgr: mgr}
	mgr.registerLogicNode("logic-1", "logic", 42)

	trans.OnClose(&stubConn{id: 42}, nil)

	if mgr.getLogicNode("logic-1") != nil {
		t.Fatal("OnClose must unregister logic-1")
	}
	if mgr.getLogicNodeByConnId(42) != nil {
		t.Fatal("OnClose must drop connMap entry")
	}
	if n := countMembers(mgr); n != 0 {
		t.Fatalf("Range count = %d, want 0", n)
	}
}

func TestOnClose_UnknownConn_NoOp(t *testing.T) {
	mgr := newLogicNodeMgr()
	trans := &transportorRpc{logicNodeMgr: mgr}
	mgr.registerLogicNode("logic-1", "logic", 42)

	trans.OnClose(&stubConn{id: 99}, nil)
	trans.OnClose(nil, nil)

	if mgr.getLogicNode("logic-1") == nil {
		t.Fatal("unknown conn must not drop logic-1")
	}
}

func TestOnClose_StaleConnKeepsReconnectedNode(t *testing.T) {
	mgr := newLogicNodeMgr()
	trans := &transportorRpc{logicNodeMgr: mgr}
	mgr.registerLogicNode("logic-1", "logic", 1)
	mgr.registerLogicNode("logic-1", "logic", 2)

	trans.OnClose(&stubConn{id: 1}, nil)

	got := mgr.getLogicNode("logic-1")
	if got == nil || got.connId != 2 {
		t.Fatalf("stale OnClose must keep new conn, got %+v", got)
	}
	if mgr.getLogicNodeByConnId(1) != nil {
		t.Fatal("stale connId must be removed from connMap")
	}

	trans.OnClose(&stubConn{id: 2}, nil)
	if mgr.getLogicNode("logic-1") != nil {
		t.Fatal("current conn close must drop node")
	}
}
