package hubtcp

import (
	"errors"
	"sync/atomic"

	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorhub"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorhub/hubproto"
	"github.com/vvisun/kkdg/kkapp/kkactor/actorremotes"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// HubClient。基于kktcp实现的注册中心客户端。
type HubClient struct {
	clients        []kknet.IClient
	remoteActorMgr *actorhub.RemoteActorMgr
	opts           Options
	autoReqId      uint64
	currentClient  int32
}

var _ actorhub.IHubClient = (*HubClient)(nil)

func NewHubClient(opts Options) *HubClient {
	return &HubClient{
		opts:           opts,
		remoteActorMgr: actorhub.NewRemoteActorMgr(),
	}
}

func (slf *HubClient) Start() error {
	if slf.opts.clientCount <= 0 {
		return errors.New("client count must be greater than 0")
	}
	hubproto.InitMsgs()
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

func (slf *HubClient) GetRemoteActorMgr() actorhub.IClientRemoteActorMgr {
	return slf.remoteActorMgr
}

func (slf *HubClient) RegisterActor(actorID kkactor.LucencyActorID) {
	req := &hubproto.RegisterActorReq{
		ReqID:  atomic.AddUint64(&slf.autoReqId, 1),
		OpCode: 1,
		ActorID: actorremotes.ActorRef{
			NodeID:   actorID.NodeID(),
			ActorKey: actorID.ActorKey(),
		},
	}
	slf.sendRequest(req)
}

func (slf *HubClient) UnregisterActor(actorID kkactor.LucencyActorID) {
	req := &hubproto.RegisterActorReq{
		ReqID:  atomic.AddUint64(&slf.autoReqId, 1),
		OpCode: 2,
		ActorID: actorremotes.ActorRef{
			NodeID:   actorID.NodeID(),
			ActorKey: actorID.ActorKey(),
		},
	}
	slf.sendRequest(req)
}

func (slf *HubClient) FindActor(actorID kkactor.LucencyActorID) {
	req := &hubproto.FindActorReq{
		ReqID: atomic.AddUint64(&slf.autoReqId, 1),
		ActorID: actorremotes.ActorRef{
			NodeID:   actorID.NodeID(),
			ActorKey: actorID.ActorKey(),
		},
	}
	slf.sendRequest(req)
}

func (slf *HubClient) GetAllActorsOfNode(nodeID string) {
	req := &hubproto.GetAllActorsOfNodeReq{
		ReqID:  atomic.AddUint64(&slf.autoReqId, 1),
		NodeID: nodeID,
	}
	slf.sendRequest(req)
}

func (slf *HubClient) sendRequest(req any) {
	curClient := atomic.AddInt32(&slf.currentClient, 1)
	curClient = curClient % int32(len(slf.clients))
	slf.clients[curClient].SendMsg(req)
}

func (slf *HubClient) onError(reqID uint64, code int, message string) {

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
	case *hubproto.ErrorResp:
		h.hubClient.onError(info.ReqID, info.Code, info.Message)
	}
}
