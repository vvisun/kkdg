package atransrelay

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kkapp/achecker"
	"github.com/vvisun/kkdg/kkapp/kkactor/transport/actortrans"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kkprocessor"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xcall"
)

// registerTimeout 是 Start 等待首次向 Hub 注册完成的上限。
const registerTimeout = 5 * time.Second

type relayWait struct {
	data []byte
	err  error
}

// Options actorshard Transport 连接中心服的选项。
type Options struct {
	HubAddr  string
	NodeType string // 选填，随注册上报中心服（日志/运维用）
}

// Transport 通过独立中心服（Hub）中转 Actor 远程消息
type Transport struct {
	nodeID   string
	registry *actortrans.MessageRegistry
	opt      Options
	stream   kkpacket.IPacket

	client  *kktcp.GnetClient
	handler *clientHandler

	receiver actortrans.IRemoteActorReceiver
	mu       sync.RWMutex

	pending  sync.Map // replyTag -> chan relayWait
	replySeq uint64
	// started 表示 Start 已调用且未 Close，是生命周期意图，只由 Start/Close 改写。
	started int32
	// registered 表示当前这条连接已完成 wireRegister，是连接可用性。
	// 断线置 0，重连后由 OnConnect 重新置 1，二者分开才能让重连自愈且不影响 Close。
	registered int32
	closedVal  int32
	// regCh 由 OnConnect 在注册帧发出后通知，Start 借此等待首次注册完成。
	regCh chan struct{}
}

var _ actortrans.IRemoteActorTransport = (*Transport)(nil)

// NewTransport 创建连接中心服的 Actor 传输层。
func NewTransport(nodeID string, registry *actortrans.MessageRegistry, opt Options) *Transport {
	if registry == nil {
		kklog.PanicLog("MessageRegistry is nil")
		return nil
	}
	if opt.HubAddr == "" {
		kklog.PanicLog("Options.HubAddr is empty")
		return nil
	}
	t := &Transport{
		nodeID:   nodeID,
		registry: registry,
		opt:      opt,
		stream:   kkpacket.DefaultStreamPacket(),
		regCh:    make(chan struct{}, 1),
	}
	t.handler = &clientHandler{t: t}
	return t
}

func (t *Transport) SetReceiver(receiver actortrans.IRemoteActorReceiver) {
	t.mu.Lock()
	t.receiver = receiver
	t.mu.Unlock()
}

func (t *Transport) Start() error {
	if !achecker.IsValidActorNodeId(t.nodeID) || t.nodeID == "" {
		return kkerrors.ErrActorInvalidNodeId
	}
	if !atomic.CompareAndSwapInt32(&t.started, 0, 1) {
		return nil
	}
	atomic.StoreInt32(&t.closedVal, 0)

	opts := kknet.ApplyOptions(
		kknet.WithRawHandler(t.handler),
		kknet.WithRpProvider(kkprocessor.NewReadProcessor),
		kknet.WithWpProvider(kkprocessor.NewWriteProcessor),
		kknet.WithRecvQueueSize(1024),
		kknet.WithWorkerQueueMaxConcurrency(1),
		kknet.WithBufferSizes(4*1024, 4*1024),
		// Hub 是本传输的唯一生命线，断了必须一直重试；默认只重试 5 次，用尽即永久失能。
		kknet.WithIsNeedReconnect(true),
		kknet.WithReconnectInterval(time.Second, -1),
		kknet.WithReconnectMaxInterval(4*time.Second),
	)
	t.client = kktcp.NewClient(t.opt.HubAddr, t.handler, opts)

	// 注册统一由 OnConnect 负责，重连时会自动再走一遍；这里只等首次注册完成。
	drainSignal(t.regCh)
	if err := t.client.Connect(); err != nil {
		atomic.StoreInt32(&t.started, 0)
		return err
	}
	select {
	case <-t.regCh:
	case <-time.After(registerTimeout):
		_ = t.client.Close()
		atomic.StoreInt32(&t.started, 0)
		return kkerrors.ErrActorRemoteTransportNotConnected
	}

	kklog.Infof("[kkactor/actorshard] connected hub=%s nodeId=%s", t.opt.HubAddr, t.nodeID)
	return nil
}

// register 发送注册帧并置可用标记。首次连接与每次重连都走这里。
func (t *Transport) register() error {
	regBytes, err := marshalBody(regBody{NodeId: t.nodeID, NodeType: t.opt.NodeType})
	if err != nil {
		return err
	}
	bb, err := packFrame(t.stream, wireRegister, regBytes)
	if err != nil {
		return err
	}
	if err := t.client.SendBuffer(bb); err != nil {
		return err
	}
	atomic.StoreInt32(&t.registered, 1)
	return nil
}

func (t *Transport) Close() error {
	if !atomic.CompareAndSwapInt32(&t.closedVal, 0, 1) {
		return nil
	}
	atomic.StoreInt32(&t.registered, 0)
	// started 只由 Start/Close 改写，断线不再清零，因此这里的 CAS 不会被 OnClose 抢先，client 必被关闭。
	if atomic.CompareAndSwapInt32(&t.started, 1, 0) {
		if t.client != nil {
			_ = t.client.Close()
		}
	}
	t.failAllPending(errors.New("actorshard: transport closed"))
	kklog.Infof("[kkactor/actorshard] closed nodeId=%s", t.nodeID)
	return nil
}

func drainSignal(ch chan struct{}) {
	select {
	case <-ch:
	default:
	}
}

func notifySignal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (t *Transport) failAllPending(err error) {
	t.pending.Range(func(key, v any) bool {
		tag, _ := key.(string)
		ch, _ := v.(chan relayWait)
		t.pending.Delete(tag)
		if ch != nil {
			select {
			case ch <- relayWait{err: err}:
			default:
			}
		}
		return true
	})
}

func (t *Transport) connected() bool {
	return atomic.LoadInt32(&t.started) == 1 &&
		atomic.LoadInt32(&t.registered) == 1 &&
		t.client != nil && t.client.IsConnected()
}

func (t *Transport) Send(target actortrans.ActorRef, msg any) error {
	if !t.connected() {
		return kkerrors.ErrActorRemoteTransportNotConnected
	}
	if !target.IsValid() {
		return kkerrors.ErrActorRemoteInvalidTarget
	}
	data, err := actortrans.EncodeRequestEnvelope(t.registry, target, msg, 0)
	if err != nil {
		return err
	}
	outBytes, err := marshalBody(relayOut{DestNodeId: target.NodeID, Payload: data})
	if err != nil {
		return err
	}
	bb, err := packFrame(t.stream, wireRelay, outBytes)
	if err != nil {
		return err
	}
	return t.client.SendBuffer(bb)
}

func (t *Transport) Request(target actortrans.ActorRef, msg any, timeout time.Duration) (any, error) {
	if !t.connected() {
		return nil, kkerrors.ErrActorRemoteTransportNotConnected
	}
	if !target.IsValid() {
		return nil, kkerrors.ErrActorRemoteInvalidTarget
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	data, err := actortrans.EncodeRequestEnvelope(t.registry, target, msg, timeout)
	if err != nil {
		return nil, err
	}
	replyTag := t.nextReplyTag()
	ch := make(chan relayWait, 1)
	t.pending.Store(replyTag, ch)
	defer t.pending.Delete(replyTag)

	outBytes, err := marshalBody(relayOut{DestNodeId: target.NodeID, ReplyTag: replyTag, Payload: data})
	if err != nil {
		return nil, err
	}
	bb, err := packFrame(t.stream, wireRelay, outBytes)
	if err != nil {
		return nil, err
	}
	if err := t.client.SendBuffer(bb); err != nil {
		return nil, err
	}
	select {
	case rw := <-ch:
		if rw.err != nil {
			return nil, rw.err
		}
		return actortrans.DecodeResponseEnvelope(t.registry, rw.data)
	case <-time.After(timeout):
		return nil, fmt.Errorf("actorshard request timeout after %s", timeout)
	}
}

func (t *Transport) RequestAsync(target actortrans.ActorRef, msg any, timeout time.Duration, callback func(result any, err error)) error {
	if !t.connected() {
		return kkerrors.ErrActorRemoteTransportNotConnected
	}
	if callback == nil {
		return kkerrors.ErrActorAsyncCallbackNil
	}
	xcall.AntsGo(func() {
		result, err := t.Request(target, msg, timeout)
		callback(result, err)
	})
	return nil
}

func (t *Transport) nextReplyTag() string {
	return fmt.Sprintf("%s.%d.%d", t.nodeID, atomic.AddUint64(&t.replySeq, 1), time.Now().UnixNano())
}

func (t *Transport) getReceiver() (actortrans.IRemoteActorReceiver, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.receiver == nil {
		return nil, kkerrors.ErrActorRemoteReceiverNotSet
	}
	return t.receiver, nil
}

func (t *Transport) completeReply(replyTag string, data []byte, err error) {
	if replyTag == "" {
		return
	}
	v, ok := t.pending.LoadAndDelete(replyTag)
	if !ok {
		return
	}
	ch, ok := v.(chan relayWait)
	if !ok {
		return
	}
	select {
	case ch <- relayWait{data: data, err: err}:
	default:
	}
}

func (t *Transport) handleInboundRelay(m relayIn) error {
	env, pl, err := actortrans.DecodeRequestEnvelope(t.registry, m.Payload)
	if err != nil {
		return err
	}
	rec, err := t.getReceiver()
	if err != nil {
		if m.ReplyTag != "" {
			raw, encErr := actortrans.EncodeResponseEnvelope(t.registry, nil, err)
			if encErr == nil {
				_ = t.sendReply(m.SrcNodeId, m.ReplyTag, raw)
			}
		}
		return err
	}
	if m.ReplyTag == "" {
		return rec.HandleRemoteSend(env.Target, pl)
	}
	to := time.Duration(env.TimeoutMs) * time.Millisecond
	if to <= 0 {
		to = 5 * time.Second
	}
	res, rerr := rec.HandleRemoteRequest(env.Target, pl, to)
	raw, encErr := actortrans.EncodeResponseEnvelope(t.registry, res, rerr)
	if encErr != nil {
		raw, _ = actortrans.EncodeResponseEnvelope(t.registry, nil, encErr)
	}
	return t.sendReply(m.SrcNodeId, m.ReplyTag, raw)
}

func (t *Transport) sendReply(destNodeId, replyTag string, payload []byte) error {
	if !t.connected() {
		return kkerrors.ErrActorRemoteTransportNotConnected
	}
	b, err := marshalBody(replyBody{DestNodeId: destNodeId, ReplyTag: replyTag, Payload: payload})
	if err != nil {
		return err
	}
	bb, err := packFrame(t.stream, wireReply, b)
	if err != nil {
		return err
	}
	return t.client.SendBuffer(bb)
}

//------------------------------------------------------------------

type clientHandler struct {
	t *Transport
}

// OnConnect 首次连接与每次重连都会触发，重新向 Hub 注册本节点。
// Hub 断开时会清掉该连接的 nodeId 绑定，不重注册则重连后 Hub 无法把消息路由回来。
func (h *clientHandler) OnConnect(c kknet.IConn) {
	if err := h.t.register(); err != nil {
		kklog.Warnf("[kkactor/actorshard] register hub failed nodeId=%s: %v", h.t.nodeID, err)
		return
	}
	notifySignal(h.t.regCh)
}

func (h *clientHandler) OnClose(c kknet.IConn, err error) {
	// 只清连接可用性，不动 started：否则 Close 的 CAS 会失败并跳过 client.Close()。
	atomic.StoreInt32(&h.t.registered, 0)
	h.t.failAllPending(errors.New("actorshard: hub connection closed"))
	kklog.Debugf("[kkactor/actorshard] hub disconnect nodeId=%s err=%v", h.t.nodeID, err)
}

func (h *clientHandler) OnRaw(connID kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	defer kkbuffer.Put(data)
	t := h.t
	wt, body, err := unpackFrame(t.stream, data.B)
	if err != nil {
		kklog.Warnf("[kkactor/actorshard] unpack: %v", err)
		return
	}
	switch wt {
	case wireRelay:
		var m relayIn
		if err := unmarshalBody(body, &m); err != nil {
			kklog.Warnf("[kkactor/actorshard] relayIn: %v", err)
			return
		}
		// 异步投递：同步 HandleRemoteRequest 会阻塞读循环，嵌套 RPC 时同连接上的 wireReply 无法入站（死锁）。
		// Payload 必须脱离 data 缓冲（OnRaw 返回后会 Put(data)）。
		if len(m.Payload) > 0 {
			m.Payload = append([]byte(nil), m.Payload...)
		}
		mm := m
		xcall.AntsGo(func() {
			if err := t.handleInboundRelay(mm); err != nil {
				kklog.Errorf("[kkactor/actorshard] handle relay: %v", err)
			}
		})
	case wireReply:
		var m replyBody
		if err := unmarshalBody(body, &m); err != nil {
			kklog.Warnf("[kkactor/actorshard] reply: %v", err)
			return
		}
		t.completeReply(m.ReplyTag, m.Payload, nil)
	case wireErr:
		var m errBody
		if err := unmarshalBody(body, &m); err != nil {
			return
		}
		t.completeReply(m.ReplyTag, nil, errors.New(m.Message))
	default:
		kklog.Warnf("[kkactor/actorshard] unknown wire %d", wt)
	}
}

func (h *clientHandler) OnNoneCopy(connID kknet.CONN_ID, data []byte) {}
