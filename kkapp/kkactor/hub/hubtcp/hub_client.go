package hubtcp

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/hub/actorhub"
	"github.com/vvisun/kkdg/kkapp/kkactor/hub/hubproto"
	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actorremotes"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kktime"
)

// clientInfo 单条 Hub 连接：客户端与鉴权状态。
type clientInfo struct {
	client   kknet.IClient
	isAuthed atomic.Bool
}

// HubClient。基于kktcp实现的注册中心客户端。
type HubClient struct {
	clientData     clientInfo
	remoteActorMgr *actorhub.RemoteActorMgr
	af             *kkactor.ActorFramework
	opts           ClientOptions
	autoReqId      uint64
	reqMap         map[uint64]any // 请求ID -> 请求数据
	muReqMap       sync.Mutex
	nodeInfo       *kkdiscovery.MemberInfo
}

var _ actorhub.IHubClient = (*HubClient)(nil)

func NewHubClient(opts ClientOptions, af *kkactor.ActorFramework, nodeInfo *kkapp.NodeInfo) *HubClient {
	info := kkdiscovery.MemberInfo{
		NodeID:     nodeInfo.GetNodeId(),
		NodeType:   nodeInfo.GetNodeType(),
		Address:    nodeInfo.GetAddress(),
		RpcAddress: nodeInfo.GetRpcAddress(),
	}
	return &HubClient{
		opts:           opts,
		remoteActorMgr: actorhub.NewRemoteActorMgr(),
		af:             af,
		reqMap:         make(map[uint64]any),
		nodeInfo:       &info,
	}
}

func (slf *HubClient) Start() error {
	hubproto.InitMsgs()

	handler := newClientHandler(slf)
	client := kktcp.NewClient(slf.opts.Addr, handler, kknet.ApplyOptions(
		kknet.WithRawHandler(handler),
		kknet.WithStreamTool(hubproto.HubMessagePacket.GetStreamTool()),
		kknet.WithMsgPacket(hubproto.HubMessagePacket.GetMessageTool()),
	))
	slf.clientData.client = client
	if err := client.Connect(); err != nil {
		return err
	}

	return nil
}

func (slf *HubClient) Stop() error {
	slf.clientData.client.Close()
	return nil
}

func (slf *HubClient) GetRemoteActorMgr() actorhub.IClientRemoteActorMgr {
	return slf.remoteActorMgr
}

func (slf *HubClient) RegisterActor(actorID kkactor.LucencyID) error {
	if !slf.anyConnAuthed() {
		return hubproto.ErrNotAuthed
	}
	req := &hubproto.RegisterActorReq{
		ReqID:  atomic.AddUint64(&slf.autoReqId, 1),
		OpCode: 1,
		ActorID: actorremotes.ActorRef{
			NodeID:   actorID.NodeID(),
			ActorKey: actorID.ActorKey(),
		},
		NodeInfo: slf.nodeInfo,
	}

	err := slf.sendRequest(req)
	if err != nil {
		return err
	}
	slf.muReqMap.Lock()
	slf.reqMap[req.ReqID] = req
	slf.muReqMap.Unlock()
	return nil
}

func (slf *HubClient) UnregisterActor(actorID kkactor.LucencyID) error {
	if !slf.anyConnAuthed() {
		return hubproto.ErrNotAuthed
	}
	req := &hubproto.RegisterActorReq{
		ReqID:  atomic.AddUint64(&slf.autoReqId, 1),
		OpCode: 2,
		ActorID: actorremotes.ActorRef{
			NodeID:   actorID.NodeID(),
			ActorKey: actorID.ActorKey(),
		},
		NodeInfo: slf.nodeInfo,
	}

	err := slf.sendRequest(req)
	if err != nil {
		return err
	}
	slf.muReqMap.Lock()
	slf.reqMap[req.ReqID] = req
	slf.muReqMap.Unlock()
	return nil
}

func (slf *HubClient) FindActor(actorID kkactor.LucencyID) error {
	if !slf.anyConnAuthed() {
		return hubproto.ErrNotAuthed
	}
	req := &hubproto.FindActorReq{
		ReqID: atomic.AddUint64(&slf.autoReqId, 1),
		ActorID: actorremotes.ActorRef{
			NodeID:   actorID.NodeID(),
			ActorKey: actorID.ActorKey(),
		},
	}

	err := slf.sendRequest(req)
	if err != nil {
		return err
	}
	slf.muReqMap.Lock()
	slf.reqMap[req.ReqID] = req
	slf.muReqMap.Unlock()
	return nil
}

func (slf *HubClient) GetAllActorsOfNode(nodeID string) error {
	if !slf.anyConnAuthed() {
		return hubproto.ErrNotAuthed
	}
	req := &hubproto.GetAllActorsOfNodeReq{
		ReqID:  atomic.AddUint64(&slf.autoReqId, 1),
		NodeID: nodeID,
	}

	err := slf.sendRequest(req)
	if err != nil {
		return err
	}
	slf.muReqMap.Lock()
	slf.reqMap[req.ReqID] = req
	slf.muReqMap.Unlock()
	return nil
}

func (slf *HubClient) anyConnAuthed() bool {
	return slf.clientData.isAuthed.Load()
}

func (slf *HubClient) sendRequest(req any) error {
	if req == nil {
		return nil
	}
	if !slf.clientData.isAuthed.Load() {
		return hubproto.ErrNotAuthed
	}
	return slf.clientData.client.SendMsg(req)
}

func (slf *HubClient) delReq(reqID uint64) {
	slf.muReqMap.Lock()
	delete(slf.reqMap, reqID)
	slf.muReqMap.Unlock()
}

func (slf *HubClient) getReq(reqID uint64) any {
	slf.muReqMap.Lock()
	req, ok := slf.reqMap[reqID]
	slf.muReqMap.Unlock()
	if ok {
		return req
	}
	return nil
}

//----------------------------------------------------------------

type clientHandler struct {
	hubClient *HubClient
}

func newClientHandler(hubClient *HubClient) *clientHandler {
	return &clientHandler{
		hubClient: hubClient,
	}
}

func (h *clientHandler) OnConnect(c kknet.IConn) {
	_ = c.SendMsg(&hubproto.AuthReq{Password: h.hubClient.opts.Password})
}

func (h *clientHandler) OnClose(kknet.IConn, error) {
	h.hubClient.clientData.isAuthed.Store(false)
}

func (h *clientHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	msg, err := kkpacket.DecodeStream(data, hubproto.HubMessagePacket.GetStreamTool(), hubproto.HubMessagePacket.GetMessageTool())
	kkbuffer.Put(data)
	if err != nil {
		return
	}
	switch info := msg.(type) {
	case *hubproto.AuthResp:
		if info.Code != 0 {
			return
		}
		h.hubClient.clientData.isAuthed.Store(true)
	case *hubproto.FindActorResp:
		h.hubClient.delReq(info.ReqID)
		if info.ErrorInfo != nil {
			return
		}
		if info.NodeInfo == nil || info.NodeInfo.RpcAddress == "" {
			return
		}
		lucId, err := kkactor.NewLucencyID(info.ActorID.NodeID, info.ActorID.ActorKey)
		if err != nil {
			return
		}
		h.hubClient.remoteActorMgr.RegisterActor(lucId, info.NodeInfo.RpcAddress)
	case *hubproto.GetAllActorsOfNodeResp:
		h.hubClient.delReq(info.ReqID)
		if info.ErrorInfo != nil {
			return
		}
		if info.NodeInfo == nil || info.NodeInfo.RpcAddress == "" {
			return
		}
		for _, actor := range info.Actors {
			lucId, err := kkactor.NewLucencyID(actor.NodeID, actor.ActorKey)
			if err != nil {
				return
			}
			h.hubClient.remoteActorMgr.RegisterActor(lucId, info.NodeInfo.RpcAddress)
		}
	case *hubproto.RegisterActorResp:
		if info.ErrorInfo == nil {
			h.hubClient.delReq(info.ReqID)
			return
		}
		kktime.GetGameTimingWheel().AfterFunc(time.Second*1, func() {
			if h.hubClient.af == nil {
				return
			}
			req := h.hubClient.getReq(info.ReqID)
			if req == nil {
				return
			}
			// 如果本地有这个actor，则重新发送请求
			if _, err := h.hubClient.af.GetLocator().GetLocalActor(&req.(*hubproto.RegisterActorReq).ActorID); err != nil {
				return
			}
			h.hubClient.sendRequest(req)
		})
	}
}
