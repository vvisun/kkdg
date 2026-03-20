package hubtcp

import (
	"errors"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorhub"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorhub/hubproto"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// HubClient。基于kktcp实现的注册中心客户端。
type HubClient struct {
	clients        []kknet.IClient
	remoteActorMgr *actorhub.RemoteActorMgr
	opts           Options
	discovery      kkdiscovery.IDiscovery
	autoReqId      uint64
	currentClient  int
}

var _ actorhub.IHubClient = (*HubClient)(nil)

func NewHubClient(opts Options, discovery kkdiscovery.IDiscovery) *HubClient {
	return &HubClient{
		opts:      opts,
		discovery: discovery,
	}
}

func (slf *HubClient) Start() error {
	if slf.opts.clientCount <= 0 {
		return errors.New("client count must be greater than 0")
	}
	slf.clients = make([]kknet.IClient, 0, slf.opts.clientCount)
	for i := 0; i < slf.opts.clientCount; i++ {
		handler := newClientHandler(slf)
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
	return nil
}

func (slf *HubClient) RegisterActor(req *hubproto.RegisterActorReq) {
	if req == nil {
		return
	}
	reqId := atomic.AddUint64(&slf.autoReqId, 1)
	req.ReqID = reqId
	slf.clients[slf.currentClient].SendMsg(req)
	slf.currentClient = (slf.currentClient + 1) % len(slf.clients)
}

func (slf *HubClient) UnregisterActor(req *hubproto.RegisterActorReq) {
	if req == nil {
		return
	}
	reqId := atomic.AddUint64(&slf.autoReqId, 1)
	req.ReqID = reqId
	slf.clients[slf.currentClient].SendMsg(req)
	slf.currentClient = (slf.currentClient + 1) % len(slf.clients)
}

func (slf *HubClient) FindActor(req *hubproto.FindActorReq) {
	if req == nil {
		return
	}
	reqId := atomic.AddUint64(&slf.autoReqId, 1)
	req.ReqID = reqId
	slf.clients[slf.currentClient].SendMsg(req)
	slf.currentClient = (slf.currentClient + 1) % len(slf.clients)
}

func (slf *HubClient) GetAllActorsOfNode(req *hubproto.GetAllActorsOfNodeReq) {
	if req == nil {
		return
	}
	reqId := atomic.AddUint64(&slf.autoReqId, 1)
	req.ReqID = reqId
	slf.clients[slf.currentClient].SendMsg(req)
	slf.currentClient = (slf.currentClient + 1) % len(slf.clients)
}

func (slf *HubClient) GetRemoteActorMgr() actorhub.IClientRemoteActorMgr {
	return slf.remoteActorMgr
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
}

func (h *clientHandler) OnClose(c kknet.IConn, err error) {
}

func (h *clientHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	msg, err := kkpacket.DecodeStream(data, hubproto.HubMessagePacket.GetStreamTool(), hubproto.HubMessagePacket.GetMessageTool())
	kkbuffer.Put(data)
	if err != nil {
		return
	}
	switch info := msg.(type) {
	case *hubproto.FindActorResp:
		lucId, err := kkactor.NewLucencyActorID(info.ActorID.NodeID, info.ActorID.ActorKey)
		if err != nil {
			return
		}
		h.hubClient.remoteActorMgr.RegisterActor(lucId, info.NodeInfo.RpcAddress)
	case *hubproto.GetAllActorsOfNodeResp:
		for _, actor := range info.Actors {
			lucId, err := kkactor.NewLucencyActorID(actor.NodeID, actor.ActorKey)
			if err != nil {
				return
			}
			h.hubClient.remoteActorMgr.RegisterActor(lucId, info.NodeInfo.RpcAddress)
		}
	}
}
