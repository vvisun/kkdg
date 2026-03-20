package hubtcp

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorhub"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorhub/hubproto"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kktime"
)

// HubClient。基于kktcp实现的注册中心客户端。
type HubClient struct {
	clients        []kknet.IClient
	remoteActorMgr *actorhub.RemoteActorMgr
	af             *kkactor.ActorFramework
	opts           Options
	autoReqId      uint64
	currentClient  int32
	authedClients  []atomic.Bool  // 与 clients 同序，该条连接是否已通过 AuthResp
	reqMap         map[uint64]any // 请求ID -> 请求数据
	muReqMap       sync.Mutex
	nodeInfo       *kkdiscovery.MemberInfo
}

var _ actorhub.IHubClient = (*HubClient)(nil)

func NewHubClient(opts Options, af *kkactor.ActorFramework, nodeInfo *kkapp.NodeInfo) *HubClient {
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
	if slf.opts.clientCount <= 0 {
		return errors.New("client count must be greater than 0")
	}
	hubproto.InitMsgs()
	slf.authedClients = make([]atomic.Bool, slf.opts.clientCount)
	slf.clients = make([]kknet.IClient, 0, slf.opts.clientCount)
	for i := 0; i < slf.opts.clientCount; i++ {
		handler := newClientHandler(slf, i)
		client := kktcp.NewClient(slf.opts.Addr, handler, kknet.ApplyOptions(
			kknet.WithRawHandler(handler),
			kknet.WithStreamTool(hubproto.HubMessagePacket.GetStreamTool()),
			kknet.WithMsgPacket(hubproto.HubMessagePacket.GetMessageTool()),
		))
		slf.clients = append(slf.clients, client)
		if err := client.Connect(); err != nil {
			return err
		}
	}
	return nil
}

func (slf *HubClient) Stop() error {
	for _, client := range slf.clients {
		client.Close()
	}
	// 鉴权计数由各自连接的 OnClose 成对扣减，这里不再 Store(0)，避免先于 OnClose 清零导致扣成负数。
	return nil
}

func (slf *HubClient) GetRemoteActorMgr() actorhub.IClientRemoteActorMgr {
	return slf.remoteActorMgr
}

func (slf *HubClient) RegisterActor(actorID kkactor.LucencyActorID) error {
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

func (slf *HubClient) UnregisterActor(actorID kkactor.LucencyActorID) error {
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

func (slf *HubClient) FindActor(actorID kkactor.LucencyActorID) error {
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
	for i := range slf.authedClients {
		if slf.authedClients[i].Load() {
			return true
		}
	}
	return false
}

func (slf *HubClient) sendRequest(req any) error {
	if req == nil {
		return nil
	}
	n := len(slf.clients)
	if n == 0 {
		return errors.New("no hub connections")
	}
	for try := 0; try < n; try++ {
		cur := int(atomic.AddInt32(&slf.currentClient, 1)) % n
		if slf.authedClients[cur].Load() {
			return slf.clients[cur].SendMsg(req)
		}
	}
	return hubproto.ErrNotAuthed
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
	idx       int
}

func newClientHandler(hubClient *HubClient, idx int) *clientHandler {
	return &clientHandler{
		hubClient: hubClient,
		idx:       idx,
	}
}

func (h *clientHandler) OnConnect(c kknet.IConn) {
	_ = c.SendMsg(&hubproto.AuthReq{Password: h.hubClient.opts.Password})
}

func (h *clientHandler) OnClose(kknet.IConn, error) {
	h.hubClient.authedClients[h.idx].Store(false)
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
		h.hubClient.authedClients[h.idx].Store(true)
	case *hubproto.FindActorResp:
		h.hubClient.delReq(info.ReqID)
		if info.ErrorInfo != nil {
			return
		}
		if info.NodeInfo == nil || info.NodeInfo.RpcAddress == "" {
			return
		}
		lucId, err := kkactor.NewLucencyActorID(info.ActorID.NodeID, info.ActorID.ActorKey)
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
			lucId, err := kkactor.NewLucencyActorID(actor.NodeID, actor.ActorKey)
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
			if _, err := h.hubClient.af.GetLocator().GetLocalActor(
				req.(*hubproto.RegisterActorReq).ActorID.NodeID,
				req.(*hubproto.RegisterActorReq).ActorID.ActorKey); err != nil {
				return
			}
			h.hubClient.sendRequest(req)
		})
	}
}
