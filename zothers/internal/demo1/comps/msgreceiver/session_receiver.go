package msgreceiver

import (
	"github.com/vvisun/kkdg/kkapp/transport/gametrans"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/utils/buffers/byteslice"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/queues/taskqueue"
)

/** 解析完整包数据[length,message]。
 *@param packet []byte 完整包数据[length,message]
 *@param packetTool *kkpacket.FullPacket 完整包工具
 *@return kkpacket.MSGID 消息ID
 *@return []byte 消息对象二进制数据
 *@return error 错误
 */
func parseMsgInfo(packet []byte, packetTool *kkpacket.FullPacket) (kkpacket.MSGID, []byte, error) {
	messageBytes, err := packetTool.GetStreamTool().Unpack(packet)
	if err != nil {
		return 0, nil, err
	}
	msgId, err := packetTool.GetMessageTool().GetMsgID(messageBytes)
	if err != nil {
		return 0, nil, err
	}
	bodyBytes, err := packetTool.GetMessageTool().BodyBytes(messageBytes)
	if err != nil {
		return 0, nil, err
	}
	return msgId, bodyBytes, nil
}

// NewSessionMsgReceiver 创建会话消息接收器
//
//	@param packetTool *kkpacket.FullPacket 完整包工具
//	@param sessionManager *gametrans.SessionManager 会话管理器。
//	因为decodeWorkers需要和sessionManager的工作线程数量一致，所以这里要求传入sessionManager，语义清晰些。
//	@return *SessionMsgReceiver 会话消息接收器
func NewSessionMsgReceiver(
	packetTool *kkpacket.FullPacket,
	sessionMgr *gametrans.SessionManager,
	decodeErrorCallback DecodeErrorCallback,
) *SessionMsgReceiver {
	workersCount := sessionMgr.GetWorkersCount()
	decodeWorkers := make([]*taskqueue.WorkerQueue, workersCount)
	for i := 0; i < workersCount; i++ {
		decodeWorkers[i] = taskqueue.NewWorkerQueue(1)
	}
	return &SessionMsgReceiver{
		packetTool:          packetTool,
		hdMap:               make(map[kkpacket.MSGID]IMsgHandler[string]),
		decodeWorkers:       decodeWorkers,
		decodeErrorCallback: decodeErrorCallback,
	}
}

func (r *SessionMsgReceiver) ThreadWorkerCount() int {
	return len(r.decodeWorkers)
}

func RegisterSessionMsgHandler[T any](receiver *SessionMsgReceiver, call MsgHandlerFunc[string, T]) {
	router := receiver.packetTool.GetMessageTool().GetRouter()
	bodyCodec := receiver.packetTool.GetMessageTool().GetBodyCodec()
	var v T
	msgID := router.GetMsgID(&v)
	if msgID == 0 {
		kklog.Error("message type not registered")
		return
	}
	h := newMsgHandler(msgID, bodyCodec, call)
	receiver.hdMap[msgID] = h
}

// SessionMsgReceiver 会话消息接收器
type SessionMsgReceiver struct {
	packetTool          *kkpacket.FullPacket
	hdMap               map[kkpacket.MSGID]IMsgHandler[string] // 消息ID到消息处理器的映射
	decodeWorkers       []*taskqueue.WorkerQueue               // 解码工作队列, 并行解码消息。用于游戏服的会话消息接收器。
	decodeErrorCallback DecodeErrorCallback
}

var _ gametrans.ISessionMsgReceiver = (*SessionMsgReceiver)(nil)
var _ gametrans.IThreadWorkerCount = (*SessionMsgReceiver)(nil)

// OnSession 实现gametrans.ISessionMsgReceiver接口。接收来自会话的消息并分发到消息处理器。
//
//	@param sessionID 会话ID
//	@param packet 整包数据[length,message]。不得保存 packet 引用，如需保存，请自行拷贝。
func (r *SessionMsgReceiver) OnSession(sessionID string, packet []byte, threadIdx int) {
	msgID, bodyBytes, err := parseMsgInfo(packet, r.packetTool)
	if err != nil {
		// 解码错误，抛给上层处理，一般是客户端发来非法数据，可能是客户端版本过低，也可能是异常攻击。
		// 上层可以返回一个错误码给客户端，然后关闭连接，这样即对客户端友好，又能防止恶意攻击。
		if r.decodeErrorCallback != nil {
			r.decodeErrorCallback(sessionID, GameErrorCodeDecodeError)
		}
		return
	}

	h, ok := r.hdMap[msgID]
	if !ok || h == nil {
		// 消息ID不存在，抛给上层处理，一般是客户端发来非法数据，可能是客户端版本过低，也可能是异常攻击。
		// 上层可以返回一个错误码给客户端，然后关闭连接，这样即对客户端友好，又能防止恶意攻击。
		if r.decodeErrorCallback != nil {
			r.decodeErrorCallback(sessionID, GameErrorCodeMsgIDNotFound)
		}
		return
	}

	if threadIdx < 0 || threadIdx >= len(r.decodeWorkers) {
		kklog.Errorf("OnSession invalid threadIdx=%d workers=%d sid=%s", threadIdx, len(r.decodeWorkers), sessionID)
		return
	}

	bodyCopy := byteslice.GetWithLenCap(len(bodyBytes), len(bodyBytes))
	copy(bodyCopy, bodyBytes)

	r.decodeWorkers[threadIdx].Push(func() {
		if err := h.OnMessage(sessionID, bodyCopy, true); err != nil {
			// 解码错误，抛给上层处理，一般是客户端发来非法数据，可能是客户端版本过低，也可能是异常攻击。
			// 上层可以返回一个错误码给客户端，然后关闭连接，这样即对客户端友好，又能防止恶意攻击。
			if r.decodeErrorCallback != nil {
				r.decodeErrorCallback(sessionID, GameErrorCodeDecodeError)
			}
		}
	})
}
