package ccgate

import (
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

type gateHandler struct {
	gate       *gateComponent
	gateNodeId string
}

var _ kknet.IConnLifecycleHandler = (*gateHandler)(nil)
var _ kknet.IRawHandler = (*gateHandler)(nil)

func newGateHandler(gate *gateComponent) *gateHandler {
	return &gateHandler{
		gate:       gate,
		gateNodeId: gate.GetApplication().GetNodeId(),
	}
}

func (h *gateHandler) OnConnect(c kknet.IConn) {
	//kklog.Debugf("[ccgate] client connected: connID=%d, remoteAddr=%s", c.ID(), c.RemoteAddr())
	if h.gate.server.GetConnManager().GetCount() >= h.gate.gateOpt.MaxConnCount {
		kklog.Debugf("[ccgate] max conn count reached, reject: remoteAddr=%s", c.RemoteAddr())
		c.Close()
		return
	}
	h.gate.onNewClientConn(c)
}

func (h *gateHandler) OnClose(c kknet.IConn, err error) {
	//kklog.Debugf("[ccgate] client disconnected: connID=%d, remoteAddr=%s, err=%v", c.ID(), c.RemoteAddr(), err)
	h.gate.onClientConnClose(c)
}

// OnRaw 收到客户端消息，转发给逻辑节点
func (h *gateHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}
	defer kkbuffer.Put(data)

	sessionID := h.gate.clientMgr.getSessionByConnId(connID)
	if sessionID == "" {
		return // 客户端已断开|已被踢出会话
	}

	// 解析消息，应该路由到哪类逻辑服
	appOpts := h.gate.GetApplication().GetOptions()
	msgBytes, err := appOpts.StreamTool.MessageBytes(data.B)
	if err != nil {
		h.gate.feedCallback(connID, ERR_CLIENT_INVALID_PACKET)
		return
	}
	msgID, err := appOpts.ClientMsgPacket.GetMsgID(msgBytes)
	if err != nil {
		h.gate.feedCallback(connID, ERR_CLIENT_INVALID_PACKET)
		return
	}
	route, err := appOpts.ClientMsgPacket.GetRouter().GetMsgRoute(msgID)
	if err != nil {
		h.gate.feedCallback(connID, ERR_CLIENT_INVALID_PACKET)
		return
	}

	// 先为client选择一个逻辑服
	logicNode := h.gate.allocLogicNode(connID, route)
	if logicNode == nil {
		// 通知客户端分配逻辑服失败
		h.gate.feedCallback(connID, ERR_ALLOC_LOGIC_NODE_FAILED)
		return
	}

	// 将客户端消息原样转发给逻辑服
	streamBytes := data.B //transportor编码时是复制，所以这里可以直接传引用，不用再复制一次。
	if err := h.gate.transportor.ForwardToLogic(sessionID, streamBytes, logicNode.nodeId); err != nil {
		// 通知业务层，转发逻辑服失败。
		h.gate.feedCallback(connID, ERR_FORWARD_LOGIC_NODE_FAILED)
	}
}
