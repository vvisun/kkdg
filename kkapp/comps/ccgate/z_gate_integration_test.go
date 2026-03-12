package ccgate

import (
	"testing"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kkapp/transport/gatetrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// mockConn is a minimal kknet.IConn for testing gateHandler.
type mockConn struct {
	id   kknet.CONN_ID
	addr string
}

func (m *mockConn) ID() kknet.CONN_ID                       { return m.id }
func (m *mockConn) Close() error                            { return nil }
func (m *mockConn) RemoteAddr() string                      { return m.addr }
func (m *mockConn) SendBuffer(_ *kkbuffer.ByteBuffer) error { return nil }
func (m *mockConn) SendMsg(_ any) error                     { return nil }
func (m *mockConn) SetExtraData(any)                        {}
func (m *mockConn) GetExtraData() any                       { return nil }

// mockTransportor records forwarded packets without real network.
type mockTransportor struct {
	lastSession string
	lastPacket  []byte
	lastLogicID string
}

func (m *mockTransportor) Stop() error { return nil }
func (m *mockTransportor) ForwardToLogic(sessionID string, packet []byte, logicNodeId string) error {
	m.lastSession = sessionID
	m.lastPacket = append([]byte(nil), packet...)
	m.lastLogicID = logicNodeId
	return nil
}
func (m *mockTransportor) ForwardToClient(string, []byte) error    { return nil }
func (m *mockTransportor) ForwardToClients([]string, []byte) error { return nil }
func (m *mockTransportor) NotifyClientDisconnect(string, string, kknet.CONN_ID) error {
	return nil
}

// mockDiscovery implements kkdiscovery.IDiscovery with a single member.
type mockMember struct {
	id     string
	nType  string
	weight int
}

func (m *mockMember) GetNodeID() string                { return m.id }
func (m *mockMember) GetNodeType() string              { return m.nType }
func (m *mockMember) GetAddress() string               { return "" }
func (m *mockMember) GetSetting(string) (string, bool) { return "", false }
func (m *mockMember) GetWeight() int                   { return m.weight }
func (m *mockMember) SetWeight(int)                    {}
func (m *mockMember) GetStatus() int                   { return kkdiscovery.NodeStatusOnline }
func (m *mockMember) SetStatus(int)                    {}

type mockMemberMgr struct {
	member kkdiscovery.IMember
}

func (m *mockMemberMgr) MemberCount() int {
	if m.member != nil {
		return 1
	}
	return 0
}
func (m *mockMemberMgr) Range(fn func(nodeID string, member kkdiscovery.IMember) bool) {
	if m.member == nil {
		return
	}
	fn(m.member.GetNodeID(), m.member)
}
func (m *mockMemberMgr) ListByType(string) []kkdiscovery.IMember {
	return []kkdiscovery.IMember{m.member}
}
func (m *mockMemberMgr) Random(string) (kkdiscovery.IMember, bool) { return m.member, m.member != nil }
func (m *mockMemberMgr) GetType(string) (string, error)            { return m.member.GetNodeType(), nil }
func (m *mockMemberMgr) GetMember(string) (kkdiscovery.IMember, bool) {
	return m.member, m.member != nil
}
func (m *mockMemberMgr) OnAddMember(kkdiscovery.MemberListener)    {}
func (m *mockMemberMgr) OnRemoveMember(kkdiscovery.MemberListener) {}

type mockDiscovery struct {
	mgr   *mockMemberMgr
	codec kkcodec.ICodec
}

func (d *mockDiscovery) Name() string { return "mock" }
func (d *mockDiscovery) Start() error { return nil }
func (d *mockDiscovery) Stop() error  { return nil }
func (d *mockDiscovery) Stats() kkdiscovery.DiscoveryStatsSnapshot {
	return kkdiscovery.DiscoveryStatsSnapshot{}
}
func (d *mockDiscovery) SetInfoGetter(func() (int, int))      {}
func (d *mockDiscovery) GetMemberMgr() kkdiscovery.IMemberMgr { return d.mgr }
func (d *mockDiscovery) IsRunning() bool                      { return true }

// Test_gateHandler_OnRaw_end_to_end verifies connect -> OnRaw -> ForwardToLogic path.
func Test_gateHandler_OnRaw_end_to_end(t *testing.T) {
	// 1) prepare global message packet & route
	router := kkpacket.NewMsgRouter()
	const (
		msgIDLogin = 1
		routeGame  = "game"
	)
	// 注册一个消息类型和路由
	type LoginReq struct {
		User string `json:"user"`
	}
	if err := router.Register(msgIDLogin, &LoginReq{}, routeGame); err != nil {
		t.Fatalf("Register: %v", err)
	}
	kkapp.ConfigDefaults(
		kkpacket.DefaultStreamPacket(),
		kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeJson),
			router,
		),
		nil,
	)

	// 2) construct gateComponent with mocks (bypassing Init/Start)
	gate := &gateComponent{}
	gate.opt.LogicNodeType = routeGame
	gate.clientMgr = newClientManager()
	gate.sessionMgr = gatetrans.NewSessionMgr()
	mt := &mockTransportor{}
	gate.transportor = mt

	// fake application identity for getSessionId
	nodeInfo := kkapp.NewNodeInfo("gate1", kkapp.NodeTypeGate, "", "", nil)
	app := component.NewApplication(nodeInfo, nil)
	gate.Component.SetApplication(app)

	// discovery: single game node
	gate.discovery = &mockDiscovery{
		mgr: &mockMemberMgr{
			member: &mockMember{id: "game1", nType: routeGame, weight: 1},
		},
	}

	handler := newGateHandler(gate)

	// 3) simulate client connect
	conn := &mockConn{id: 100, addr: "mock:0"}
	handler.OnConnect(conn)

	// 4) encode a login message using kkapp.GetMsgPacket
	packetBuf, err := kkpacket.EncodeStream(&LoginReq{User: "u1"}, kkapp.GetStreamTool(), kkapp.GetMsgPacket())
	if err != nil {
		t.Fatalf("EncodeStream: %v", err)
	}

	// 5) feed raw packet into handler
	handler.OnRaw(conn.ID(), packetBuf)

	// 6) verify that ForwardToLogic was called with expected logic node and session
	if mt.lastLogicID != "game1" {
		t.Fatalf("ForwardToLogic logicNodeId = %q, want %q", mt.lastLogicID, "game1")
	}
	if mt.lastSession == "" {
		t.Fatalf("ForwardToLogic sessionID should not be empty")
	}
	if len(mt.lastPacket) == 0 {
		t.Fatalf("ForwardToLogic packet should not be empty")
	}
}

// Benchmark the throughput of gateHandler.OnRaw (logic requests per second).
func Benchmark_gateHandler_OnRaw_throughput(b *testing.B) {
	// Reuse the same setup as the integration test.
	router := kkpacket.NewMsgRouter()
	const (
		msgIDLogin = 1
		routeGame  = "game"
	)
	type LoginReq struct {
		User string `json:"user"`
	}
	if err := router.Register(msgIDLogin, &LoginReq{}, routeGame); err != nil {
		b.Fatalf("Register: %v", err)
	}
	kkapp.ConfigDefaults(
		kkpacket.DefaultStreamPacket(),
		kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeJson),
			router,
		),
		nil,
	)

	gate := &gateComponent{}
	gate.opt.LogicNodeType = routeGame
	gate.clientMgr = newClientManager()
	gate.sessionMgr = gatetrans.NewSessionMgr()
	mt := &mockTransportor{}
	gate.transportor = mt
	nodeInfo := kkapp.NewNodeInfo("gate1", kkapp.NodeTypeGate, "", "", nil)
	app := component.NewApplication(nodeInfo, nil)
	gate.Component.SetApplication(app)
	gate.discovery = &mockDiscovery{
		mgr: &mockMemberMgr{
			member: &mockMember{id: "game1", nType: routeGame, weight: 1},
		},
	}
	handler := newGateHandler(gate)

	// Single mock connection; we only care about handler cost, not real network.
	conn := &mockConn{id: 100, addr: "mock:0"}
	handler.OnConnect(conn)

	// Pre-encode a packet to avoid counting encoding cost.
	packetBuf, err := kkpacket.EncodeStream(&LoginReq{User: "bench"}, kkapp.GetStreamTool(), kkapp.GetMsgPacket())
	if err != nil {
		b.Fatalf("EncodeStream: %v", err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			// Note: OnRaw takes ownership of buffer and returns it,
			// so we must clone underlying bytes per call.
			cloned := kkbuffer.GetWithCapacity(len(packetBuf.B))
			cloned.B = append(cloned.B[:0], packetBuf.B...)
			handler.OnRaw(conn.ID(), cloned)
		}
	})
}

// Benchmark including message encoding + OnRaw, closer to real end-to-end cost inside gate.
func Benchmark_gateHandler_OnRaw_withEncode(b *testing.B) {
	router := kkpacket.NewMsgRouter()
	const (
		msgIDLogin = 1
		routeGame  = "game"
	)
	type LoginReq struct {
		User string `json:"user"`
	}
	if err := router.Register(msgIDLogin, &LoginReq{}, routeGame); err != nil {
		b.Fatalf("Register: %v", err)
	}
	kkapp.ConfigDefaults(
		kkpacket.DefaultStreamPacket(),
		kkpacket.NewMessagePacket(
			kkpacket.NewPacketHead(&kkpacket.PartUint32{}),
			kkcodec.GetCodec(kkcodec.CodecTypeJson),
			router,
		),
		nil,
	)

	gate := &gateComponent{}
	gate.opt.LogicNodeType = routeGame
	gate.clientMgr = newClientManager()
	gate.sessionMgr = gatetrans.NewSessionMgr()
	mt := &mockTransportor{}
	gate.transportor = mt
	nodeInfo := kkapp.NewNodeInfo("gate1", kkapp.NodeTypeGate, "", "", nil)
	app := component.NewApplication(nodeInfo, nil)
	gate.Component.SetApplication(app)
	gate.discovery = &mockDiscovery{
		mgr: &mockMemberMgr{
			member: &mockMember{id: "game1", nType: routeGame, weight: 1},
		},
	}
	handler := newGateHandler(gate)
	conn := &mockConn{id: 100, addr: "mock:0"}
	handler.OnConnect(conn)

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			bb, err := kkpacket.EncodeStream(&LoginReq{User: "bench"}, kkapp.GetStreamTool(), kkapp.GetMsgPacket())
			if err != nil {
				b.Fatalf("EncodeStream: %v", err)
			}
			handler.OnRaw(conn.ID(), bb)
		}
	})
}
