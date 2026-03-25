package gametransshard

import (
	"time"

	"github.com/vvisun/kkdg/kkapp/transport/ptotrans"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

//--------------------------------- 网关连接管理 ---------------------------------

type gatewayClient struct {
	shardIdx int
	trans    *transportorShard
	cli      kknet.IClient
}

func NewGatewayClient(shardIdx int, trans *transportorShard) *gatewayClient {
	handler := &gatewayHandler{shardIdx: shardIdx}
	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(handler),
		kknet.WithRpProvider(kkprocessor.NewReadProcessor),
		kknet.WithWpProvider(kkprocessor.NewWriteProcessor),
		kknet.WithRecvQueueSize(4096),
		kknet.WithWorkerQueueMaxConcurrency(1),
		kknet.WithBufferSizes(16*1024, 16*1024),
		kknet.WithIsNeedReconnect(true),
		kknet.WithReconnectInterval(1*time.Second, -1),
		kknet.WithReconnectMaxInterval(4*time.Second),
	)
	cli := kktcp.NewClient(trans.gatewayAddr, handler, opts)
	cliGateway := &gatewayClient{shardIdx: shardIdx, trans: trans, cli: cli}
	handler.cli = cliGateway
	go cliGateway.connect()
	return cliGateway
}

func (slf *gatewayClient) connect() {
	for {
		if slf.trans.stopped {
			kklog.Warnf("[分流%d] 分流客户端已停止，停止连接网关循环", slf.shardIdx)
			return
		}
		if err := slf.cli.Connect(); err == nil {
			kklog.Infof("分流[%d] 连接网关成功", slf.shardIdx)
			return
		}
		kklog.Infof("分流[%d] 连接网关失败，重试中", slf.shardIdx)
		time.Sleep(1 * time.Second)
	}
}

func (slf *gatewayClient) IsConnected() bool {
	if slf.cli == nil {
		return false
	}
	return slf.cli.IsConnected()
}

type gatewayHandler struct {
	shardIdx int
	cli      *gatewayClient
}

func (h *gatewayHandler) OnConnect(conn kknet.IConn) {
	// 将自己注册到网关
	go func() {
		//循环注册到网关，直到成功为止
		for {
			if h.cli.trans.stopped {
				kklog.Warnf("[分流%d] 分流客户端已停止，停止注册到网关循环", h.shardIdx)
				return
			}
			err := h.sendRpcMsgRegister()
			if err == nil {
				kklog.Infof("[分流%d] 注册到网关成功", h.shardIdx)
				return
			}
			kklog.Warnf("[分流%d] 注册到网关失败，重试中", h.shardIdx)
			time.Sleep(1 * time.Second)
		}
	}()
}

// 将自己注册到网关
func (h *gatewayHandler) sendRpcMsgRegister() error {
	msg := ptotrans.RpcMsgRegister{
		ShardIdx: int32(h.shardIdx),
		NodeId:   h.cli.trans.nodeId,
		NodeType: h.cli.trans.nodeType,
	}
	transStreamTool := h.cli.trans.transStreamTool
	transMsgPacket := h.cli.trans.transMsgPacket
	bb, err := kkpacket.EncodeStream(&msg, transStreamTool, transMsgPacket)
	if err != nil {
		kklog.Warnf("[分流%d] 编码 RpcMsgRegister: %v", h.shardIdx, err)
		return err
	}
	err = h.cli.cli.SendBuffer(bb)
	if err != nil {
		kklog.Warnf("[分流%d] 发送 RpcMsgRegister: %v", h.shardIdx, err)
		return err
	}
	return nil
}

func (h *gatewayHandler) OnClose(conn kknet.IConn, err error) {
	kklog.Infof("分流[%d] 网关连接断开: %v", h.shardIdx, err)
}

func (h *gatewayHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	defer kkbuffer.Put(data)
	transStreamTool := h.cli.trans.transStreamTool
	transMsgPacket := h.cli.trans.transMsgPacket
	pkt := data.B
	messageBytes, err := transStreamTool.MessageBytes(pkt)
	if err != nil {
		return
	}
	msgID, err := transMsgPacket.GetMsgID(messageBytes)
	if err != nil {
		kklog.Warnf("[分流%d] GetMsgID: %v", h.shardIdx, err)
		return
	}
	bodyBytes, err := transMsgPacket.BodyBytes(messageBytes)
	if err != nil {
		return
	}

	trans := h.cli.trans

	switch msgID {
	case ptotrans.MsgIDRpcC2S: // 网关转发客户端消息到逻辑服: 客户端->网关->逻辑服
		var msg ptotrans.RpcC2S
		err = transMsgPacket.GetBodyCodec().Unmarshal(bodyBytes, &msg)
		if err != nil {
			kklog.Warnf("[分流%d] 解析 RpcC2S: %v", h.shardIdx, err)
			return
		}
		sessionInfo := trans.sessionMgr.GetSession(msg.ClientId)
		if sessionInfo == nil {
			sessionInfo = trans.sessionMgr.AddSessionWithShard(msg.ClientId, msg.GateNodeId, h.shardIdx)
		}
		// 注意：这里的trans.msgReceiver是外部传进来的一个指针，所以存在多个分流客户端共用一个msgReceiver的情况。
		trans.msgReceiver.OnSession(msg.ClientId, msg.Payload, sessionInfo.GetThreadIdx())
	case ptotrans.MsgIDRpcClientDisconnect: // 客户端断开事件
		var msg ptotrans.RpcClientDisconnect
		err = transMsgPacket.GetBodyCodec().Unmarshal(bodyBytes, &msg)
		if err != nil {
			kklog.Warnf("[分流%d] 解析 RpcClientDisconnect: %v", h.shardIdx, err)
			return
		}
		kklog.Debugf("[分流%d] 客户端断开 clientId=%s clientIds=%v", h.shardIdx, msg.ClientId, msg.ClientIds)
		trans.sessionMgr.RemoveSession(msg.ClientId)
		for _, clientId := range msg.ClientIds {
			trans.sessionMgr.RemoveSession(clientId)
		}
	}
}

func (h *gatewayHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {

}
