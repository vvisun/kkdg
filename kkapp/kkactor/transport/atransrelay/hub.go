package atransrelay

import (
	"errors"
	"sync"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// Hub Actor 远程消息中心服：各节点接入后注册 NodeId，所有 Actor 消息经本服务路由。
type Hub struct {
	addr   string
	stream kkpacket.IPacket
	srv    kknet.IServer

	mu    sync.RWMutex
	nodes map[string]*peerSession // nodeId -> session

	byConn sync.Map // kknet.CONN_ID -> *peerSession
}

// NewHub 创建中心服。listenAddr 为 TCP 监听地址，例如 ":9100"。
func NewHub(listenAddr string) *Hub {
	return &Hub{
		addr:   listenAddr,
		stream: kkpacket.DefaultStreamPacket(),
		nodes:  make(map[string]*peerSession),
	}
}

// Addr 返回监听地址（Start 前为构造传入值）。
func (h *Hub) Addr() string {
	return h.addr
}

// Start 在 listenAddr 上启动 TCP 服务。
func (h *Hub) Start() error {
	handler := &hubSrvHandler{hub: h}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(handler),
		kknet.WithRpProvider(kkprocessor.NewReadProcessor),
		kknet.WithWpProvider(kkprocessor.NewWriteProcessor),
		kknet.WithRecvQueueSize(1024),
		kknet.WithWorkerQueueMaxConcurrency(1),
		kknet.WithBufferSizes(4*1024, 4*1024),
	)
	h.srv = kktcp.NewServer(h.addr, handler, opts)
	if err := h.srv.Start(); err != nil {
		return err
	}
	if h.srv != nil {
		h.addr = h.srv.Addr()
	}
	kklog.Infof("[actorhub] listening %s", h.addr)
	return nil
}

// Stop 停止中心服。
func (h *Hub) Stop() error {
	if h.srv == nil {
		return nil
	}
	return h.srv.Stop()
}

type peerSession struct {
	hub    *Hub
	conn   kknet.IConn
	connID kknet.CONN_ID
	nodeID string

	writeMu sync.Mutex
}

func (s *peerSession) sendFrame(wireType byte, body []byte) error {
	bb, err := packFrame(s.hub.stream, wireType, body)
	if err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	c := s.conn
	if c == nil {
		kkbuffer.Put(bb)
		return errors.New("peer session closed")
	}
	return c.SendBuffer(bb)
}

func (s *peerSession) sendErr(replyTag, message string) {
	b, err := marshalBody(errBody{ReplyTag: replyTag, Message: message})
	if err != nil {
		return
	}
	_ = s.sendFrame(wireErr, b)
}

type hubSrvHandler struct {
	hub *Hub
}

func (h *hubSrvHandler) OnConnect(c kknet.IConn) {
	s := &peerSession{hub: h.hub, conn: c, connID: c.ID()}
	h.hub.byConn.Store(c.ID(), s)
}

func (h *hubSrvHandler) OnClose(c kknet.IConn, err error) {
	v, _ := h.hub.byConn.LoadAndDelete(c.ID())
	if v == nil {
		return
	}
	s := v.(*peerSession)
	h.hub.unregister(s)
	kklog.Debugf("[actorhub] peer close nodeId=%q connId=%d err=%v", s.nodeID, s.connID, err)
	s.writeMu.Lock()
	s.conn = nil
	s.writeMu.Unlock()
}

func (h *hubSrvHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	defer kkbuffer.Put(data)
	v, ok := h.hub.byConn.Load(connID)
	if !ok {
		return
	}
	s := v.(*peerSession)
	h.hub.dispatch(s, data.B)
}

func (h *hubSrvHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {}

func (h *Hub) unregister(s *peerSession) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if s.nodeID != "" {
		if cur, ok := h.nodes[s.nodeID]; ok && cur == s {
			delete(h.nodes, s.nodeID)
		}
	}
	s.nodeID = ""
}

func (h *Hub) bind(s *peerSession, nodeID string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if old, ok := h.nodes[nodeID]; ok && old != s {
		go func(oc kknet.IConn) {
			if oc != nil {
				_ = oc.Close()
			}
		}(old.conn)
		delete(h.nodes, nodeID)
	}
	h.nodes[nodeID] = s
	s.nodeID = nodeID
}

func (h *Hub) getSession(nodeID string) *peerSession {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.nodes[nodeID]
}

func (h *Hub) dispatch(s *peerSession, packet []byte) {
	wt, body, err := unpackFrame(h.stream, packet)
	if err != nil {
		kklog.Warnf("[actorhub] unpack: %v", err)
		return
	}
	switch wt {
	case wireRegister:
		var m regBody
		if err := unmarshalBody(body, &m); err != nil {
			kklog.Warnf("[actorhub] register decode: %v", err)
			return
		}
		if m.NodeId == "" {
			s.sendErr("", "empty node id")
			return
		}
		h.bind(s, m.NodeId)
		kklog.Infof("[actorhub] registered nodeId=%s nodeType=%s", m.NodeId, m.NodeType)
	case wireRelay:
		if s.nodeID == "" {
			s.sendErr("", "relay before register")
			return
		}
		var out relayOut
		if err := unmarshalBody(body, &out); err != nil {
			kklog.Warnf("[actorhub] relay decode: %v", err)
			return
		}
		if out.DestNodeId == "" || len(out.Payload) == 0 {
			s.sendErr(out.ReplyTag, "invalid relay dest or payload")
			return
		}
		dest := h.getSession(out.DestNodeId)
		if dest == nil {
			s.sendErr(out.ReplyTag, "dest offline: "+out.DestNodeId)
			return
		}
		in := relayIn{
			SrcNodeId:  s.nodeID,
			DestNodeId: out.DestNodeId,
			ReplyTag:   out.ReplyTag,
			Payload:    out.Payload,
		}
		b, err := marshalBody(in)
		if err != nil {
			s.sendErr(out.ReplyTag, "hub marshal: "+err.Error())
			return
		}
		if err := dest.sendFrame(wireRelay, b); err != nil {
			kklog.Warnf("[actorhub] forward relay to %s: %v", out.DestNodeId, err)
			s.sendErr(out.ReplyTag, "forward failed: "+err.Error())
		}
	case wireReply:
		if s.nodeID == "" {
			s.sendErr("", "reply before register")
			return
		}
		var m replyBody
		if err := unmarshalBody(body, &m); err != nil {
			kklog.Warnf("[actorhub] reply decode: %v", err)
			return
		}
		if m.DestNodeId == "" || m.ReplyTag == "" {
			return
		}
		dest := h.getSession(m.DestNodeId)
		if dest == nil {
			return
		}
		b, err := marshalBody(m)
		if err != nil {
			return
		}
		_ = dest.sendFrame(wireReply, b)
	default:
		kklog.Warnf("[actorhub] unknown wire type %d", wt)
	}
}
