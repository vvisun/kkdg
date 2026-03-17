package ccgate

import (
	"testing"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/transport"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// fake implementations for black-box testing of gateHandler.OnRaw.

type fakeConn struct {
	id   kknet.CONN_ID
	data [][]byte
}

func (c *fakeConn) ID() kknet.CONN_ID                         { return c.id }
func (c *fakeConn) RemoteAddr() string                        { return "fake" }
func (c *fakeConn) Close() error                              { return nil }
func (c *fakeConn) SetExtraData(userData any)                 {}
func (c *fakeConn) GetExtraData() any                         { return nil }
func (c *fakeConn) SendBuffer(buf *kkbuffer.ByteBuffer) error { c.data = append(c.data, buf.B); return nil }
func (c *fakeConn) SendMsg(msg any) error                     { return nil }

type fakeTransportor struct {
	forwardedSessionID string
	forwardedNodeID    string
	forwardedPacket    []byte
}

func (t *fakeTransportor) Stop() error { return nil }
func (t *fakeTransportor) ForwardToLogic(sessionID string, packet []byte, logicNodeId string) error {
	t.forwardedSessionID = sessionID
	t.forwardedNodeID = logicNodeId
	t.forwardedPacket = append([]byte(nil), packet...)
	return nil
}
func (t *fakeTransportor) ForwardToClient(sessionID string, packet []byte) error   { return nil }
func (t *fakeTransportor) ForwardToClients(sessionIDs []string, packet []byte) error {
	return nil
}
func (t *fakeTransportor) NotifyClientDisconnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error {
	return nil
}
func (t *fakeTransportor) NotifyClientConnect(sessionID string, logicNodeId string, connId kknet.CONN_ID) error {
	return nil
}
func (t *fakeTransportor) HookMsg(listener gatetrans.MsgHookListener) {}

// fake application embedding only what gateComponent needs.
type fakeApp struct {
	opts kkapp.AppOptions
	nid  string
	nt   string
	ni   *kkapp.NodeInfo
	pid  *actor.PID
}

func (a *fakeApp) GetOptions() *kkapp.AppOptions           { return &a.opts }
func (a *fakeApp) GetNodeId() string                       { return a.nid }
func (a *fakeApp) GetNodeType() string                     { return a.nt }
func (a *fakeApp) GetNodeInfo() *kkapp.NodeInfo            { return a.ni }
func (a *fakeApp) Receive(actor.Context)                   {}
func (a *fakeApp) GetPID() *actor.PID                      { return a.pid }
func (a *fakeApp) GetCompPID(string) *actor.PID            { return nil }
func (a *fakeApp) Start() error                            { return nil }
func (a *fakeApp) Stop() error                             { return nil }
func (a *fakeApp) AddComponent(kkapp.IComponent) error     { return nil }
func (a *fakeApp) SetConfigDir(string)                     {}
func (a *fakeApp) GetConfigDir() string                    { return "" }

// message type used in tests/benchmarks
type testMsg struct {
	Hello string
}

// Test that OnRaw routes and forwards to logic as expected.
func TestGateHandler_OnRaw_RouteAndForward(t *testing.T) {
	// prepare app options with a router that routes msgID=1 to "game"
	appOpts := kkapp.DefaultOptions()
	const routeType = "game"
	const msgID kkpacket.MSGID = 1
	// register message type and route on router
	if err := appOpts.ClientMsgPacket.GetRouter().Register(msgID, &testMsg{}, routeType); err != nil {
		t.Fatalf("register route failed: %v", err)
	}

	// build gate component with fake application attached via embedding
	gate := &gateComponent{
		opt:        Option{TransType: transport.TransTypeNats, MaxConnCount: 10},
		localDis:   newLocalDiscovery(),
		sessionMgr: gatetrans.NewSessionMgr(),
		clientMgr:  newClientManager(),
		userMgr:    newUserManager(),
		logicBindMgr: newLogicBindManager(),
	}

	// attach fake app by setting the embedded Component's application field via facade
	fa := &fakeApp{
		opts: appOpts,
		nid:  "gate-1",
		nt:   "gate",
		ni:   kkapp.NewNodeInfo("gate-1", "gate", "", "", nil),
	}
	gate.SetApplication(fa)

	// fake transportor and server/conn
	ft := &fakeTransportor{}
	gate.transportor = ft

	clientConn := &fakeConn{id: 1}

	// simulate new connection to init session/client mapping
	gate.onNewClientConn(clientConn)
	sessionID := getSessionId(clientConn.ID(), gate.GetApplication().GetNodeId())

	// ensure chooseLogicNode returns a stable nodeId to verify binding
	const logicNodeID = "logic-1"
	gate.discovery = nil // force use of chooseFromShardOrRpc/chooseFromDiscovery skip; we'll bypass and directly bind
	gate.logicBindMgr.sessionBind(sessionID, routeType, logicNodeID)

	handler := newGateHandler(gate)

	// build a raw client packet
	msg := &testMsg{Hello: "world"}
	bb, err := kkpacket.EncodeStream(msg, appOpts.StreamTool, appOpts.ClientMsgPacket)
	if err != nil {
		t.Fatalf("encode msg failed: %v", err)
	}
	streamPacket := append([]byte(nil), bb.B...)
	buf := kkbuffer.NewByteBuffer(streamPacket)

	// call OnRaw
	handler.OnRaw(clientConn.ID(), buf)

	// verify forwarded info
	if ft.forwardedSessionID != sessionID {
		t.Fatalf("expected sessionID %q, got %q", sessionID, ft.forwardedSessionID)
	}
	if ft.forwardedNodeID != logicNodeID {
		t.Fatalf("expected logicNodeID %q, got %q", logicNodeID, ft.forwardedNodeID)
	}
	if string(ft.forwardedPacket) != string(streamPacket) {
		t.Fatalf("expected forwarded packet %v, got %v", streamPacket, ft.forwardedPacket)
	}
}

// Benchmark the critical path from OnRaw to ForwardToLogic.
func BenchmarkGateHandler_OnRaw(b *testing.B) {
	// setup once
	appOpts := kkapp.DefaultOptions()
	const routeType = "game"
	const msgID kkpacket.MSGID = 1
	if err := appOpts.ClientMsgPacket.GetRouter().Register(msgID, &testMsg{}, routeType); err != nil {
		b.Fatalf("register route failed: %v", err)
	}

	gate := &gateComponent{
		opt:        Option{TransType: transport.TransTypeNats, MaxConnCount: 100000},
		localDis:   newLocalDiscovery(),
		sessionMgr: gatetrans.NewSessionMgr(),
		clientMgr:  newClientManager(),
		userMgr:    newUserManager(),
		logicBindMgr: newLogicBindManager(),
	}

	fa := &fakeApp{
		opts: appOpts,
		nid:  "gate-bench",
		nt:   "gate",
		ni:   kkapp.NewNodeInfo("gate-bench", "gate", "", "", nil),
	}
	gate.SetApplication(fa)

	ft := &fakeTransportor{}
	gate.transportor = ft

	clientConn := &fakeConn{id: 1}
	gate.onNewClientConn(clientConn)
	sessionID := getSessionId(clientConn.ID(), gate.GetApplication().GetNodeId())

	const logicNodeID = "logic-bench"
	gate.logicBindMgr.sessionBind(sessionID, routeType, logicNodeID)

	handler := newGateHandler(gate)

	// prebuild one stream packet to reuse in benchmark
	msg := &testMsg{Hello: "world"}
	bb, err := kkpacket.EncodeStream(msg, appOpts.StreamTool, appOpts.ClientMsgPacket)
	if err != nil {
		b.Fatalf("encode msg failed: %v", err)
	}
	streamPacket := append([]byte(nil), bb.B...)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		buf := kkbuffer.NewByteBuffer(streamPacket)
		handler.OnRaw(clientConn.ID(), buf)
	}
}


