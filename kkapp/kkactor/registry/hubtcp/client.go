package hubtcp

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkapp/kkactor"
	"github.com/vvisun/kkdg/kkapp/kkactor/registry/actorhub"
	"github.com/vvisun/kkdg/kkapp/kkactor/registry/hubproto"
	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actortrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
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
	// 本客户端已注册到Hub的actor。服务端在连接断开时会注销该连接上的全部actor，
	// 重连鉴权成功后需要凭这份清单重新注册，否则目录里永久缺失。
	ownedActors map[kkactor.LucencyID]struct{}
	muOwned     sync.Mutex
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
		ownedActors:    make(map[kkactor.LucencyID]struct{}),
	}
}

func (slf *HubClient) Start() error {
	hubproto.InitMsgs()

	handler := newClientHandler(slf)
	client := kktcp.NewClient(slf.opts.Addr, handler, kknet.ApplyOptions(
		kknet.WithRawHandler(handler),
		kknet.WithStreamTool(hubproto.HubMessagePacket.GetStreamTool()),
		kknet.WithMsgPacket(hubproto.HubMessagePacket.GetMessageTool()),
		kknet.WithIsNeedReconnect(slf.opts.NeedReconnect),
		kknet.WithReconnectInterval(slf.opts.ReconnectInterval, slf.opts.ReconnectMaxRetries),
		kknet.WithReconnectMaxInterval(slf.opts.ReconnectMaxInterval),
	))
	slf.clientData.client = client
	if err := client.Connect(); err != nil {
		return err
	}

	return nil
}

// Stop 关闭连接。Close 会置连接状态，重连循环不会再启动。
// 允许在 Start 之前或 Start 失败后调用：此时没有连接可关，视为已停止。
func (slf *HubClient) Stop() error {
	if slf.clientData.client == nil {
		return nil
	}
	if err := slf.clientData.client.Close(); err != nil && !errors.Is(err, kkerrors.ErrNetClientNotConnected) {
		return err
	}
	return nil
}

func (slf *HubClient) GetRemoteActorMgr() actorhub.IClientRemoteActorMgr {
	return slf.remoteActorMgr
}

// newRegisterReq 构造注册/注销请求。opCode: 1注册, 2注销。
func (slf *HubClient) newRegisterReq(actorID kkactor.LucencyID, opCode int) *hubproto.RegisterActorReq {
	return &hubproto.RegisterActorReq{
		ReqID:  atomic.AddUint64(&slf.autoReqId, 1),
		OpCode: opCode,
		ActorID: actortrans.ActorRef{
			NodeID:   actorID.NodeID(),
			ActorKey: actorID.ActorKey(),
		},
		NodeInfo: slf.nodeInfo,
	}
}

func (slf *HubClient) ReqRegisterActor(actorID kkactor.LucencyID) error {
	if !slf.anyConnAuthed() {
		return hubproto.ErrNotAuthed
	}
	req := slf.newRegisterReq(actorID, 1)

	err := slf.sendRequest(req)
	if err != nil {
		return err
	}
	slf.muReqMap.Lock()
	slf.reqMap[req.ReqID] = req
	slf.muReqMap.Unlock()

	slf.muOwned.Lock()
	slf.ownedActors[actorID] = struct{}{}
	slf.muOwned.Unlock()
	return nil
}

func (slf *HubClient) ReqUnregisterActor(actorID kkactor.LucencyID) error {
	if !slf.anyConnAuthed() {
		return hubproto.ErrNotAuthed
	}
	req := slf.newRegisterReq(actorID, 2)

	err := slf.sendRequest(req)
	if err != nil {
		return err
	}
	slf.muReqMap.Lock()
	slf.reqMap[req.ReqID] = req
	slf.muReqMap.Unlock()

	slf.muOwned.Lock()
	delete(slf.ownedActors, actorID)
	slf.muOwned.Unlock()
	return nil
}

func (slf *HubClient) ReqFindActor(actorID kkactor.LucencyID) error {
	if !slf.anyConnAuthed() {
		return hubproto.ErrNotAuthed
	}
	req := &hubproto.FindActorReq{
		ReqID: atomic.AddUint64(&slf.autoReqId, 1),
		ActorID: actortrans.ActorRef{
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

func (slf *HubClient) ReqGetAllActorsOfNode(nodeID string) error {
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

// clearReqMap 连接断开时丢弃所有在途请求。这些请求不会再收到响应，
// 留着既是泄漏，也会让重试定时器拿到过期请求继续重发。
func (slf *HubClient) clearReqMap() {
	slf.muReqMap.Lock()
	if len(slf.reqMap) > 0 {
		slf.reqMap = make(map[uint64]any)
	}
	slf.muReqMap.Unlock()
}

// resendOwnedActors 重连鉴权成功后重新注册本客户端登记过的actor。
func (slf *HubClient) resendOwnedActors() {
	slf.muOwned.Lock()
	ids := make([]kkactor.LucencyID, 0, len(slf.ownedActors))
	for id := range slf.ownedActors {
		ids = append(ids, id)
	}
	slf.muOwned.Unlock()

	for _, id := range ids {
		req := slf.newRegisterReq(id, 1)
		if err := slf.sendRequest(req); err != nil {
			kklog.Warnf("[hubtcp] 重连后重注册actor失败 nodeId=%s actorKey=%s: %v", id.NodeID(), id.ActorKey(), err)
			continue
		}
		slf.muReqMap.Lock()
		slf.reqMap[req.ReqID] = req
		slf.muReqMap.Unlock()
	}
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
	h.hubClient.clearReqMap()
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
		// 首次鉴权时清单为空；重连鉴权后凭清单补回服务端已注销的actor。
		h.hubClient.resendOwnedActors()
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
			regReq, ok := req.(*hubproto.RegisterActorReq)
			if !ok {
				return
			}
			// 如果本地有这个actor，则重新发送请求
			if _, err := h.hubClient.af.GetLocalActorMgr().GetLocalActor(&regReq.ActorID); err != nil {
				return
			}
			if err := h.hubClient.sendRequest(regReq); err != nil {
				kklog.Warnf("[hubtcp] 重试注册actor失败 reqID=%d: %v", info.ReqID, err)
			}
		})
	}
}
