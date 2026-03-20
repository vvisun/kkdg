package hubtcp

import (
	"sync"

	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/hub/actorhub"
	"github.com/vvisun/kkdg/kkapp/kkactor/hub/hubproto"
	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actorremotes"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

var _ actorhub.IHubServer = (*HubServer)(nil)

// HubServer。基于 kktcp 的注册中心：每条连接需先通过 AuthReq，再处理业务。
// 注册依赖 RegisterActorReq.NodeInfo（RpcAddress 等），不要求 kkdiscovery。
type HubServer struct {
	srv            *kktcp.Server
	handler        *serverHandler
	remoteActorMgr *actorhub.RemoteActorMgr
	opts           ServerOptions
}

// NewHubServer 创建 Hub TCP 服务端。
func NewHubServer(opts ServerOptions) *HubServer {
	hub := &HubServer{
		remoteActorMgr: actorhub.NewRemoteActorMgr(),
		opts:           opts,
	}
	hub.handler = newServerHandler(hub)
	hub.srv = kktcp.NewServer(opts.Addr, hub.handler, kknet.ApplyOptions(
		kknet.WithRawHandler(hub.handler),
		kknet.WithStreamTool(hubproto.HubMessagePacket.GetStreamTool()),
		kknet.WithMsgPacket(hubproto.HubMessagePacket.GetMessageTool()),
	))
	return hub
}

// ServerRemoteActorMgr 返回服务端 actor 注册表（测试/运维）。
func (h *HubServer) ServerRemoteActorMgr() actorhub.IServerRemoteActorMgr {
	return h.remoteActorMgr
}

func (h *HubServer) Start() error {
	hubproto.InitMsgs()
	return h.srv.Start()
}

func (h *HubServer) Stop() error {
	return h.srv.Stop()
}

func (h *HubServer) Addr() string {
	if h.srv == nil {
		return ""
	}
	return h.srv.Addr()
}

//----------------------------------------------------------------

type serverHandler struct {
	hub      *HubServer
	mu       sync.Mutex
	authed   map[kknet.CONN_ID]struct{}
	connRegs map[kknet.CONN_ID]map[kkactor.LucencyID]struct{}
	// 节点维度的最近一次 Register 上报（用于 Find / List 回填完整 MemberInfo）
	nodeMeta map[string]kkdiscovery.MemberInfo
}

func newServerHandler(hub *HubServer) *serverHandler {
	return &serverHandler{
		hub:      hub,
		authed:   make(map[kknet.CONN_ID]struct{}),
		connRegs: make(map[kknet.CONN_ID]map[kkactor.LucencyID]struct{}),
		nodeMeta: make(map[string]kkdiscovery.MemberInfo),
	}
}

func (h *serverHandler) OnConnect(kknet.IConn) {}

func (h *serverHandler) OnClose(c kknet.IConn, _ error) {
	id := c.ID()
	h.mu.Lock()
	regs := h.connRegs[id]
	delete(h.connRegs, id)
	delete(h.authed, id)
	h.mu.Unlock()
	for lucID := range regs {
		_ = h.hub.remoteActorMgr.UnregisterActor(lucID)
	}
}

func (h *serverHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	msg, err := kkpacket.DecodeStream(data, hubproto.HubMessagePacket.GetStreamTool(), hubproto.HubMessagePacket.GetMessageTool())
	kkbuffer.Put(data)
	if err != nil {
		return
	}
	switch m := msg.(type) {
	case *hubproto.AuthReq:
		h.onAuthReq(connID, m)
	case *hubproto.RegisterActorReq:
		h.onRegisterActorReq(connID, m)
	case *hubproto.FindActorReq:
		h.onFindActorReq(connID, m)
	case *hubproto.GetAllActorsOfNodeReq:
		h.onGetAllActorsOfNodeReq(connID, m)
	default:
		return
	}
}

func (h *serverHandler) onAuthReq(connID kknet.CONN_ID, req *hubproto.AuthReq) {
	if req.Password != h.hub.opts.Password {
		_ = h.hub.srv.SendMsg(connID, &hubproto.AuthResp{
			Code:    hubproto.ErrorCodeFailed,
			Message: "auth failed",
		})
		return
	}
	h.mu.Lock()
	h.authed[connID] = struct{}{}
	h.mu.Unlock()
	_ = h.hub.srv.SendMsg(connID, &hubproto.AuthResp{
		Code:    hubproto.ErrorCodeSuccess,
		Message: "",
	})
}

func (h *serverHandler) requireAuthed(connID kknet.CONN_ID) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.authed[connID]
	return ok
}

func (h *serverHandler) onRegisterActorReq(connID kknet.CONN_ID, req *hubproto.RegisterActorReq) {
	if !h.requireAuthed(connID) {
		_ = h.hub.srv.SendMsg(connID, &hubproto.RegisterActorResp{
			ReqID:   req.ReqID,
			OpCode:  req.OpCode,
			ActorID: req.ActorID,
			ErrorInfo: &hubproto.ErrorResp{
				Code:    hubproto.ErrorCodeFailed,
				Message: "not authed",
			},
		})
		return
	}

	lucID, err := kkactor.NewLucencyID(req.ActorID.NodeID, req.ActorID.ActorKey)
	if err != nil {
		_ = h.replyRegisterErr(connID, req, err.Error())
		return
	}

	if req.OpCode == 1 {
		if req.NodeInfo == nil {
			_ = h.replyRegisterErr(connID, req, "nodeInfo required for register")
			return
		}
		if req.NodeInfo.RpcAddress == "" {
			_ = h.replyRegisterErr(connID, req, "nodeInfo.rpcAddress required")
			return
		}
		nid := req.NodeInfo.NodeID
		if nid == "" {
			nid = req.ActorID.NodeID
		}
		if nid != req.ActorID.NodeID {
			_ = h.replyRegisterErr(connID, req, "nodeInfo.nodeID must match actorID.nodeID")
			return
		}
	}

	rpcAddr := ""
	if req.NodeInfo != nil {
		rpcAddr = req.NodeInfo.RpcAddress
	}

	switch req.OpCode {
	case 1:
		if err := h.hub.remoteActorMgr.RegisterActor(lucID, rpcAddr); err != nil {
			_ = h.replyRegisterErr(connID, req, err.Error())
			return
		}
		h.mu.Lock()
		if h.connRegs[connID] == nil {
			h.connRegs[connID] = make(map[kkactor.LucencyID]struct{})
		}
		h.connRegs[connID][lucID] = struct{}{}
		if req.NodeInfo != nil {
			meta := *req.NodeInfo
			if meta.NodeID == "" {
				meta.NodeID = req.ActorID.NodeID
			}
			h.nodeMeta[req.ActorID.NodeID] = meta
		}
		h.mu.Unlock()
	case 2:
		if err := h.hub.remoteActorMgr.UnregisterActor(lucID); err != nil {
			_ = h.replyRegisterErr(connID, req, err.Error())
			return
		}
		h.mu.Lock()
		if regs := h.connRegs[connID]; regs != nil {
			delete(regs, lucID)
		}
		h.mu.Unlock()
	default:
		_ = h.replyRegisterErr(connID, req, "invalid opCode")
		return
	}

	_ = h.hub.srv.SendMsg(connID, &hubproto.RegisterActorResp{
		ReqID:     req.ReqID,
		OpCode:    req.OpCode,
		ActorID:   req.ActorID,
		ErrorInfo: nil,
	})
}

func (h *serverHandler) replyRegisterErr(connID kknet.CONN_ID, req *hubproto.RegisterActorReq, msg string) error {
	return h.hub.srv.SendMsg(connID, &hubproto.RegisterActorResp{
		ReqID:   req.ReqID,
		OpCode:  req.OpCode,
		ActorID: req.ActorID,
		ErrorInfo: &hubproto.ErrorResp{
			Code:    hubproto.ErrorCodeFailed,
			Message: msg,
		},
	})
}

func (h *serverHandler) onFindActorReq(connID kknet.CONN_ID, req *hubproto.FindActorReq) {
	if !h.requireAuthed(connID) {
		_ = h.hub.srv.SendMsg(connID, &hubproto.FindActorResp{
			ReqID:   req.ReqID,
			ActorID: req.ActorID,
			ErrorInfo: &hubproto.ErrorResp{
				Code:    hubproto.ErrorCodeFailed,
				Message: "not authed",
			},
		})
		return
	}
	lucID, err := kkactor.NewLucencyID(req.ActorID.NodeID, req.ActorID.ActorKey)
	if err != nil {
		_ = h.replyFindErr(connID, req.ReqID, req.ActorID, err.Error())
		return
	}
	ref := actorremotes.ActorRef{NodeID: lucID.NodeID(), ActorKey: lucID.ActorKey()}
	ra, err := h.hub.remoteActorMgr.FindActor(lucID)
	if err != nil {
		_ = h.replyFindErr(connID, req.ReqID, ref, err.Error())
		return
	}
	ni := h.nodeInfoForLookup(ra)
	_ = h.hub.srv.SendMsg(connID, &hubproto.FindActorResp{
		ReqID:     req.ReqID,
		ActorID:   ref,
		NodeInfo:  ni,
		ErrorInfo: nil,
	})
}

func (h *serverHandler) replyFindErr(connID kknet.CONN_ID, reqID uint64, ref actorremotes.ActorRef, msg string) error {
	return h.hub.srv.SendMsg(connID, &hubproto.FindActorResp{
		ReqID:     reqID,
		ActorID:   ref,
		ErrorInfo: &hubproto.ErrorResp{Code: hubproto.ErrorCodeFailed, Message: msg},
	})
}

func (h *serverHandler) onGetAllActorsOfNodeReq(connID kknet.CONN_ID, req *hubproto.GetAllActorsOfNodeReq) {
	if !h.requireAuthed(connID) {
		_ = h.hub.srv.SendMsg(connID, &hubproto.GetAllActorsOfNodeResp{
			ReqID: req.ReqID,
			ErrorInfo: &hubproto.ErrorResp{
				Code:    hubproto.ErrorCodeFailed,
				Message: "not authed",
			},
		})
		return
	}
	list := h.hub.remoteActorMgr.GetAllActorsOfNode(req.NodeID)
	refs := make([]*actorremotes.ActorRef, 0, len(list))
	for _, ra := range list {
		id := ra.LucencyID()
		ref := actorremotes.ActorRef{NodeID: id.NodeID(), ActorKey: id.ActorKey()}
		cpy := ref
		refs = append(refs, &cpy)
	}
	var ni *kkdiscovery.MemberInfo
	h.mu.Lock()
	if meta, ok := h.nodeMeta[req.NodeID]; ok {
		cpy := meta
		ni = &cpy
	}
	h.mu.Unlock()
	if ni == nil && len(list) > 0 {
		ni = nodeInfoFromRemoteActor(list[0])
	}
	if ni == nil {
		ni = &kkdiscovery.MemberInfo{NodeID: req.NodeID}
	}
	_ = h.hub.srv.SendMsg(connID, &hubproto.GetAllActorsOfNodeResp{
		ReqID:     req.ReqID,
		NodeInfo:  ni,
		Actors:    refs,
		ErrorInfo: nil,
	})
}

func (h *serverHandler) nodeInfoForLookup(ra *actorhub.RemoteActor) *kkdiscovery.MemberInfo {
	id := ra.LucencyID()
	h.mu.Lock()
	if meta, ok := h.nodeMeta[id.NodeID()]; ok {
		cpy := meta
		h.mu.Unlock()
		return &cpy
	}
	h.mu.Unlock()
	return nodeInfoFromRemoteActor(ra)
}

func nodeInfoFromRemoteActor(ra *actorhub.RemoteActor) *kkdiscovery.MemberInfo {
	id := ra.LucencyID()
	return &kkdiscovery.MemberInfo{
		NodeID:     id.NodeID(),
		RpcAddress: ra.RpcAddress(),
	}
}
