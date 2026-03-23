package cnats

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kkmetrics"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/kktime"
	"github.com/vvisun/kkdg/utils/queues/taskqueue"
	"github.com/vvisun/kkdg/utils/xcall"
)

// NatsCluster 基于NATS的集群实现
type NatsCluster struct {
	nodeID   string // 节点ID
	nodeType string // 节点类型（用于订阅类型主题）
	conn     *nats.Conn

	discovery kkdiscovery.IDiscovery //可选，用于检查节点是否存在

	stopCh chan struct{}

	// 请求响应处理
	requestSub   *nats.Subscription
	responseSub  *nats.Subscription
	requestMap   map[string]chan *kkcluster.ClusterResponse //同步请求的响应通道
	requestMapMu sync.RWMutex
	reqMap       map[string]*asyncReq //异步请求的回调函数
	reqMu        sync.Mutex

	// 发布消息订阅
	publishSub *nats.Subscription
	// 类型发布消息订阅
	typePublishSub *nats.Subscription

	// 发布消息处理器. 外部设置，内部调用
	publishHandler func(nodeID string, packet *kkcluster.ClusterPacket)
	// 请求处理器. 外部设置，内部调用
	requestHandler func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error)

	// 订阅管理（用于重连时重新订阅）
	subMu sync.Mutex

	// 统计信息
	stats kkcluster.ClusterStats

	// 配置
	options     nats.Options
	workerQueue *taskqueue.WorkerQueue
	msgCodec    kkcodec.ICodec

	// 指标监听ID
	metricsListenerID uint64

	// lifecycle state: 0=init, 1=started, 2=stopped
	state int32
}

var _ kkcluster.ICluster = (*NatsCluster)(nil)

// asyncReq 异步请求，直接用 callback 避免 channel 分配；timer 用于超时，响应先到时需 Stop 取消
type asyncReq struct {
	cb    func(data []byte, errCode kkcluster.ClusterErrorCode)
	timer interface{ Stop() bool } // *timingwheel.Timer，响应到达时 Stop 避免重复回调
}

var asyncReqPool = sync.Pool{
	New: func() interface{} { return &asyncReq{} },
}

// NewNatsCluster 创建新的NATS集群
func NewNatsCluster(nodeID string, nodeType string, clusterOpt kkcluster.ClusterOption) kkcluster.ICluster {
	kkcluster.CheckClusterOption(&clusterOpt)
	natsOpts := FromClusterOption(clusterOpt)

	if natsOpts.Url == "" {
		kklog.Warnf("NatsCluster(%s) nats url is empty, will not start cluster", nodeID)
		return nil
	}

	return &NatsCluster{
		nodeID:      nodeID,
		nodeType:    nodeType,
		discovery:   clusterOpt.Discovery,
		requestMap:  make(map[string]chan *kkcluster.ClusterResponse),
		reqMap:      make(map[string]*asyncReq),
		stopCh:      make(chan struct{}),
		options:     natsOpts,
		workerQueue: taskqueue.NewWorkerQueue(1),
		msgCodec:    clusterOpt.MsgCodec,
	}
}

// SetRequestHandler 设置请求处理器
func (c *NatsCluster) SetRequestHandler(handler kkcluster.FunRequestHandler) {
	c.requestHandler = handler
}

// SetPublishHandler 设置发布消息处理器
func (c *NatsCluster) SetPublishHandler(handler kkcluster.FunPublishHandler) {
	c.publishHandler = handler
}

// Start 初始化集群
func (c *NatsCluster) Start() error {
	// idempotent start: already started -> no-op
	if !atomic.CompareAndSwapInt32(&c.state, 0, 1) &&
		!atomic.CompareAndSwapInt32(&c.state, 2, 1) {
		return nil
	}

	// support start after stop by recreating stopCh
	select {
	case <-c.stopCh:
		c.stopCh = make(chan struct{})
	default:
	}

	kklog.Infof("NatsCluster(%s) startup, addr=%s", c.nodeID, c.options.Url)
	if c.options.Url == "" {
		atomic.StoreInt32(&c.state, 0)
		return errors.New("nats addr is empty")
	}
	c.metricsListenerID = kkmetrics.GlobalEventMgr.Subscribe(kkmetrics.EventClusterMetrics, func(e *kkmetrics.MetricsEventData) {
		snap := c.Stats()
		e.Metrics = kkcluster.MetricsFromSnapshot(e.Namespace, snap)
	})
	if err := c.connectAndSubscribe(); err != nil {
		kkmetrics.GlobalEventMgr.UnsubscribeByID(kkmetrics.EventClusterMetrics, c.metricsListenerID)
		c.metricsListenerID = 0
		atomic.StoreInt32(&c.state, 0)
		return err
	}
	return nil
}

// connectAndSubscribe 连接NATS并订阅主题
func (c *NatsCluster) connectAndSubscribe() error {
	opts := &c.options

	// 设置重连处理器
	opts.ReconnectedCB = func(nc *nats.Conn) {
		kklog.Infof("NatsCluster(%s) reconnected to %s", c.nodeID, nc.ConnectedUrl())
		c.stats.AddReconnect()
		// 重连后重新订阅
		if err := c.resubscribe(); err != nil {
			kklog.Errorf("NatsCluster(%s) resubscribe failed: %v", c.nodeID, err)
			c.stats.AddError()
		}
	}

	// 设置断开连接处理器
	opts.DisconnectedErrCB = func(nc *nats.Conn, err error) {
		if err != nil {
			kklog.Warnf("NatsCluster(%s) disconnected: %v", c.nodeID, err)
		} else {
			kklog.Warnf("NatsCluster(%s) disconnected", c.nodeID)
		}
	}

	// 设置关闭处理器
	opts.ClosedCB = func(nc *nats.Conn) {
		kklog.Infof("NatsCluster(%s) connection closed", c.nodeID)
	}

	// 连接到NATS
	conn, err := opts.Connect()
	if err != nil {
		return err
	}
	c.conn = conn

	// 订阅主题
	return c.resubscribe()
}

// resubscribe 重新订阅所有主题
func (c *NatsCluster) resubscribe() error {
	c.subMu.Lock()
	defer c.subMu.Unlock()

	if c.conn == nil || !c.conn.IsConnected() {
		return nil
	}

	// 取消旧的订阅
	if c.requestSub != nil {
		_ = c.requestSub.Unsubscribe()
		c.requestSub = nil
	}
	if c.responseSub != nil {
		_ = c.responseSub.Unsubscribe()
		c.responseSub = nil
	}
	if c.publishSub != nil {
		_ = c.publishSub.Unsubscribe()
		c.publishSub = nil
	}
	if c.typePublishSub != nil {
		_ = c.typePublishSub.Unsubscribe()
		c.typePublishSub = nil
	}

	// 订阅请求主题（用于接收其他节点的请求）
	requestSubject := c.getRequestSubject()
	sub, err := c.conn.Subscribe(requestSubject, c.handleRequest)
	if err != nil {
		return err
	}
	c.requestSub = sub

	// 订阅发布消息主题（用于接收其他节点发送的消息）
	publishSubject := c.getPublishSubject(c.nodeID)
	publishSub, err := c.conn.Subscribe(publishSubject, c.handlePublish)
	if err != nil {
		_ = sub.Unsubscribe()
		return err
	}
	c.publishSub = publishSub

	// 订阅类型发布消息主题（用于接收同类型节点的消息）
	// 如果节点类型已设置，则订阅类型主题
	if c.nodeType != "" {
		typeSubject := c.getPublishTypeSubject(c.nodeType)
		// 使用普通订阅（Subscribe），这样所有同类型节点都能收到消息
		// 如果需要负载均衡（消息只被一个节点接收），应使用 QueueSubscribe
		typePublishSub, err := c.conn.Subscribe(typeSubject, c.handleTypePublish)
		if err != nil {
			_ = sub.Unsubscribe()
			_ = publishSub.Unsubscribe()
			return err
		}
		c.typePublishSub = typePublishSub
	}

	// 单订阅响应主题：只订阅本节点发起请求的响应
	responseSubject := c.getResponseSubjectPattern()
	respSub, err := c.conn.Subscribe(responseSubject, c.handleResponse)
	if err != nil {
		_ = sub.Unsubscribe()
		_ = publishSub.Unsubscribe()
		if c.typePublishSub != nil {
			_ = c.typePublishSub.Unsubscribe()
			c.typePublishSub = nil
		}
		return err
	}
	c.responseSub = respSub

	return nil
}

func (c *NatsCluster) IsConnected() bool {
	return c.conn != nil && c.conn.IsConnected()
}

// PublishRemote 发布消息到指定节点
func (c *NatsCluster) PublishRemote(nodeID string, packet *kkcluster.ClusterPacket) error {
	if packet == nil {
		return kkerrors.ErrClusterInvalidPacket
	}
	if !c.IsConnected() {
		return kkerrors.ErrClusterNotConnected
	}

	// 检查目标节点是否存在
	if c.discovery != nil {
		_, found := c.discovery.GetMemberMgr().GetMember(nodeID)
		if !found {
			return kkerrors.ErrClusterMemberNotFound
		}
	}

	// 设置源节点ID
	packet.SourcePath = c.nodeID
	packet.TargetPath = nodeID

	// 序列化消息
	data, err := c.msgCodec.Marshal(packet)
	kkcluster.PutClusterPacket(packet)
	if err != nil {
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) marshal publish packet failed: targetNode=%s err=%v", c.nodeID, nodeID, err)
		return err
	}

	// 发布到目标节点的主题
	subject := c.getPublishSubject(nodeID)
	if err := c.conn.Publish(subject, data); err != nil {
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) publish failed: subject=%s targetNode=%s bytes=%d err=%v", c.nodeID, subject, nodeID, len(data), err)
		return err
	}

	// 记录统计
	c.stats.AddPublishSent(len(data))
	return nil
}

// PublishRemoteType 发布消息到指定类型的所有节点
// 优化：只需发布一次到类型主题，所有订阅了该类型主题的节点都会收到消息
// 注意：节点在 Init() 时会订阅自己类型的主题，使用普通 Subscribe（不是 QueueSubscribe）
// 如果需要负载均衡（消息只被一个节点接收），应使用 QueueSubscribe
func (c *NatsCluster) PublishRemoteType(nodeType string, packet *kkcluster.ClusterPacket) error {
	if packet == nil {
		return kkerrors.ErrClusterInvalidPacket
	}
	if !c.IsConnected() {
		return kkerrors.ErrClusterNotConnected
	}

	// 检查该类型是否有节点（可选，用于提前验证）
	if c.discovery != nil {
		if c.discovery.GetMemberMgr().CountOfType(nodeType) == 0 {
			return kkerrors.ErrClusterNoMemberOfType
		}
	}

	// 设置源节点ID
	packet.SourcePath = c.nodeID
	packet.TargetPath = nodeType

	// 序列化消息（只序列化一次）
	data, err := c.msgCodec.Marshal(packet)
	kkcluster.PutClusterPacket(packet)
	if err != nil {
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) marshal type publish packet failed: nodeType=%s err=%v", c.nodeID, nodeType, err)
		return err
	}

	// 使用普通 Publish，同类型所有订阅节点都会收到
	// 若需负载均衡（消息只被一个节点接收），应使用 QueueSubscribe 订阅
	subject := c.getPublishTypeSubject(nodeType)
	if err := c.conn.Publish(subject, data); err != nil {
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) publish type failed: subject=%s nodeType=%s bytes=%d err=%v", c.nodeID, subject, nodeType, len(data), err)
		return err
	}

	// 记录统计
	c.stats.AddPublishSent(len(data))
	return nil
}

// RequestRemoteAsync 异步请求（不阻塞），结果通过 callback 回调。使用 reqMap 避免 channel 分配
func (c *NatsCluster) RequestRemoteAsync(nodeID string, packet *kkcluster.ClusterPacket, callback func(data []byte, errCode kkcluster.ClusterErrorCode), timeout ...time.Duration) error {
	if packet == nil {
		return kkerrors.ErrClusterInvalidPacket
	}
	if callback == nil {
		return kkcluster.ErrFromCode(kkcluster.ClusterErrorCodeInvalidRequest)
	}
	if !c.IsConnected() {
		return kkerrors.ErrClusterNotConnected
	}

	if c.discovery != nil {
		_, found := c.discovery.GetMemberMgr().GetMember(nodeID)
		if !found {
			return kkerrors.ErrClusterMemberNotFound
		}
	}

	reqTimeout := defaultRequestTimeout
	if len(timeout) > 0 && timeout[0] > 0 {
		reqTimeout = timeout[0]
	}
	packet.Timeout = int64(reqTimeout.Milliseconds())

	requestID := c.generateRequestID()

	pending := asyncReqPool.Get().(*asyncReq)
	pending.cb = callback
	pending.timer = nil
	c.reqMu.Lock()
	c.reqMap[requestID] = pending
	c.reqMu.Unlock()

	reqMsg := &kkcluster.ClusterRequest{
		RequestID:    requestID,
		SourceNodeID: c.nodeID,
		Packet:       packet,
	}

	data, err := c.msgCodec.Marshal(reqMsg)
	kkcluster.PutClusterPacket(packet)
	if err != nil {
		c.reqMu.Lock()
		delete(c.reqMap, requestID)
		c.reqMu.Unlock()
		pending.cb, pending.timer = nil, nil
		asyncReqPool.Put(pending)
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) marshal async request failed: requestID=%s targetNode=%s err=%v", c.nodeID, requestID, nodeID, err)
		return err
	}

	requestSubject := c.getRequestSubjectForNode(nodeID)
	if err := c.conn.Publish(requestSubject, data); err != nil {
		c.reqMu.Lock()
		delete(c.reqMap, requestID)
		c.reqMu.Unlock()
		pending.cb, pending.timer = nil, nil
		asyncReqPool.Put(pending)
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) publish async request failed: subject=%s requestID=%s targetNode=%s bytes=%d err=%v", c.nodeID, requestSubject, requestID, nodeID, len(data), err)
		return err
	}

	c.stats.AddRequestSent(len(data))

	// 使用时间轮统一调度超时，避免每个异步请求起一个 goroutine
	tw := kktime.GetNetTimingWheel()
	timer := tw.AfterFunc(reqTimeout, func() {
		c.reqMu.Lock()
		p := c.reqMap[requestID]
		delete(c.reqMap, requestID)
		c.reqMu.Unlock()
		if p != nil {
			c.stats.AddError()
			cb := p.cb
			p.cb, p.timer = nil, nil
			asyncReqPool.Put(p)
			xcall.AntsGo(func() {
				defer func() {
					if r := recover(); r != nil {
						c.stats.AddError()
						kklog.Errorf("NatsCluster RequestRemoteAsync callback panic: %v", r)
					}
				}()
				cb(nil, kkcluster.ClusterErrorCodeTimeout)
			})
		}
	})
	c.reqMu.Lock()
	if p := c.reqMap[requestID]; p != nil {
		p.timer = timer
	}
	c.reqMu.Unlock()

	return nil
}

// request 使用 NATS 原生 Request/Reply（服务端 handleRequest 在 msg.Reply 非空时走 msg.Respond）。
// 与 RequestRemote（自定义 kkcluster.response.* + channel 等待）相对，用于性能对比或可切换路径。
func (c *NatsCluster) request(nodeID string, packet *kkcluster.ClusterPacket, timeout ...time.Duration) ([]byte, kkcluster.ClusterErrorCode) {
	if packet == nil {
		return nil, kkcluster.ClusterErrorCodeInvalidRequest
	}
	if !c.IsConnected() {
		return nil, kkcluster.ClusterErrorCodeNotConnected
	}
	if c.discovery != nil {
		_, found := c.discovery.GetMemberMgr().GetMember(nodeID)
		if !found {
			return nil, kkcluster.ClusterErrorCodeMemberNotFound
		}
	}

	reqTimeout := defaultRequestTimeout
	if len(timeout) > 0 && timeout[0] > 0 {
		reqTimeout = timeout[0]
	}
	packet.Timeout = int64(reqTimeout.Milliseconds())
	requestID := c.generateRequestID()

	reqMsg := &kkcluster.ClusterRequest{
		RequestID:    requestID,
		SourceNodeID: c.nodeID,
		Packet:       packet,
	}
	data, err := c.msgCodec.Marshal(reqMsg)
	kkcluster.PutClusterPacket(packet)
	if err != nil {
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) marshal request failed: requestID=%s targetNode=%s err=%v", c.nodeID, requestID, nodeID, err)
		return nil, kkcluster.ClusterErrorCodeMarshalFailed
	}

	requestSubject := c.getRequestSubjectForNode(nodeID)
	msg, err := c.conn.Request(requestSubject, data, reqTimeout)
	if err != nil {
		c.stats.AddError()
		if errors.Is(err, nats.ErrTimeout) || errors.Is(err, nats.ErrNoResponders) {
			return nil, kkcluster.ClusterErrorCodeTimeout
		}
		kklog.Errorf("NatsCluster(%s) nats.Request failed: subject=%s err=%v", c.nodeID, requestSubject, err)
		return nil, kkcluster.ClusterErrorCodePublishFailed
	}

	c.stats.AddRequestSent(len(data))

	var resp kkcluster.ClusterResponse
	if err := c.msgCodec.Unmarshal(msg.Data, &resp); err != nil {
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) unmarshal nats reply failed: %v", c.nodeID, err)
		return nil, kkcluster.ClusterErrorCodeInvalidResponse
	}
	c.stats.AddResponseReceived(len(resp.Data))
	return resp.Data, kkcluster.ClusterErrorCode(resp.Code)
}

// RequestRemote 请求消息（带响应）
func (c *NatsCluster) RequestRemote(nodeID string, packet *kkcluster.ClusterPacket, timeout ...time.Duration) ([]byte, kkcluster.ClusterErrorCode) {
	if packet == nil {
		return nil, kkcluster.ClusterErrorCodeInvalidRequest
	}
	if !c.IsConnected() {
		return nil, kkcluster.ClusterErrorCodeNotConnected
	}

	// 检查目标节点是否存在
	if c.discovery != nil {
		_, found := c.discovery.GetMemberMgr().GetMember(nodeID)
		if !found {
			return nil, kkcluster.ClusterErrorCodeMemberNotFound
		}
	}

	// 设置超时
	reqTimeout := defaultRequestTimeout
	if len(timeout) > 0 && timeout[0] > 0 {
		reqTimeout = timeout[0]
	}
	packet.Timeout = int64(reqTimeout.Milliseconds())

	// 生成请求ID
	requestID := c.generateRequestID()

	// 创建响应通道（不关闭，避免超时后晚到响应向已关闭 channel 发送导致 panic）
	responseCh := make(chan *kkcluster.ClusterResponse, 1)
	c.requestMapMu.Lock()
	c.requestMap[requestID] = responseCh
	c.requestMapMu.Unlock()

	// 确保清理
	defer func() {
		c.requestMapMu.Lock()
		delete(c.requestMap, requestID)
		c.requestMapMu.Unlock()
	}()

	// 创建请求消息
	reqMsg := &kkcluster.ClusterRequest{
		RequestID:    requestID,
		SourceNodeID: c.nodeID,
		Packet:       packet,
	}

	// 序列化请求
	data, err := c.msgCodec.Marshal(reqMsg)
	kkcluster.PutClusterPacket(packet)
	if err != nil {
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) marshal request failed: requestID=%s targetNode=%s err=%v", c.nodeID, requestID, nodeID, err)
		return nil, kkcluster.ClusterErrorCodeMarshalFailed
	}

	// 发布请求到目标节点的请求主题
	requestSubject := c.getRequestSubjectForNode(nodeID)
	if err := c.conn.Publish(requestSubject, data); err != nil {
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) publish request failed: subject=%s requestID=%s targetNode=%s bytes=%d err=%v", c.nodeID, requestSubject, requestID, nodeID, len(data), err)
		return nil, kkcluster.ClusterErrorCodePublishFailed
	}

	// 记录发送请求统计
	c.stats.AddRequestSent(len(data))

	// 等待响应
	select {
	case resp := <-responseCh:
		// 记录接收响应统计
		c.stats.AddResponseReceived(len(resp.Data))
		return resp.Data, kkcluster.ClusterErrorCode(resp.Code)
	case <-time.After(reqTimeout):
		c.stats.AddError()
		return nil, kkcluster.ClusterErrorCodeTimeout
	}
}

// Stop 停止集群
func (c *NatsCluster) Stop() {
	// idempotent stop: only transition from started -> stopped executes cleanup
	if !atomic.CompareAndSwapInt32(&c.state, 1, 2) {
		return
	}

	kklog.Infof("NatsCluster(%s) shutdown", c.nodeID)

	kkmetrics.GlobalEventMgr.UnsubscribeByID(kkmetrics.EventClusterMetrics, c.metricsListenerID)
	c.metricsListenerID = 0

	select {
	case <-c.stopCh:
		return
	default:
		close(c.stopCh)
	}

	if c.requestSub != nil {
		_ = c.requestSub.Unsubscribe()
	}
	if c.responseSub != nil {
		_ = c.responseSub.Unsubscribe()
	}
	if c.publishSub != nil {
		_ = c.publishSub.Unsubscribe()
	}
	if c.typePublishSub != nil {
		_ = c.typePublishSub.Unsubscribe()
	}

	// 快速释放等待中的请求，避免依赖超时
	c.closeAllPendingRequests()

	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// closeAllPendingRequests 释放 requestMap 和 reqMap 中的等待请求
func (c *NatsCluster) closeAllPendingRequests() {
	// 同步请求：向每个 channel 发送 ConnClosed 响应
	c.requestMapMu.Lock()
	for id, ch := range c.requestMap {
		delete(c.requestMap, id)
		resp := &kkcluster.ClusterResponse{
			RequestID: id,
			Code:      int32(kkcluster.ClusterErrorCodeConnClosed),
			Data:      nil,
		}
		select {
		case ch <- resp:
		default:
			// channel 已满或接收方已放弃，忽略
		}
	}
	c.requestMapMu.Unlock()

	// 异步请求：调用 callback 并 Stop timer
	c.reqMu.Lock()
	for id, p := range c.reqMap {
		delete(c.reqMap, id)
		if p.timer != nil {
			p.timer.Stop()
		}
		if p.cb != nil {
			cb := p.cb
			p.cb, p.timer = nil, nil
			asyncReqPool.Put(p)
			xcall.AntsGo(func() {
				defer func() {
					if r := recover(); r != nil {
						kklog.Errorf("NatsCluster(%s) closeAllPendingRequests callback panic: %v", c.nodeID, r)
					}
				}()
				cb(nil, kkcluster.ClusterErrorCodeConnClosed)
			})
		} else {
			p.cb, p.timer = nil, nil
			asyncReqPool.Put(p)
		}
	}
	c.reqMu.Unlock()
}

// Stats 获取统计信息快照
func (c *NatsCluster) Stats() kkcluster.ClusterStatsSnapshot {
	isConnected := c.conn != nil && c.conn.IsConnected()
	return c.stats.Snapshot(isConnected)
}
