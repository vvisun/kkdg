package cnats

import (
	"strconv"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xcall"
)

// NatsCluster 基于NATS的集群实现
type NatsCluster struct {
	nodeID    string // 节点ID
	nodeType  string // 节点类型（用于订阅类型主题）
	discovery kkdiscovery.IDiscovery
	conn      *nats.Conn

	// 请求响应处理
	requestSub  *nats.Subscription
	responseSub *nats.Subscription
	requestMap  map[string]chan *kkcluster.ClusterResponse
	requestMu   sync.RWMutex
	reqMap      map[string]*asyncReq
	reqMu       sync.Mutex

	// 发布消息订阅
	publishSub *nats.Subscription

	// 类型发布消息订阅（使用队列组）
	typePublishSub *nats.Subscription

	// 发布消息处理器
	publishHandler func(nodeID string, packet *kkcluster.ClusterPacket)

	// 请求处理器
	requestHandler func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error)

	// 订阅管理（用于重连时重新订阅）
	subMu sync.Mutex

	// 统计信息
	stats kkcluster.ClusterStats

	stopCh chan struct{}

	options nats.Options
}

var _ kkcluster.ICluster = (*NatsCluster)(nil)

// asyncReq 异步请求，直接用 callback 避免 channel 分配
type asyncReq struct {
	cb func(data []byte, errCode kkcluster.ClusterErrorCode)
}

// NewNatsCluster 创建新的NATS集群
func NewNatsCluster(nodeID string, nodeType string, discovery kkdiscovery.IDiscovery, options nats.Options) kkcluster.ICluster {
	return &NatsCluster{
		nodeID:     nodeID,
		nodeType:   nodeType,
		discovery:  discovery,
		requestMap: make(map[string]chan *kkcluster.ClusterResponse),
		reqMap:     make(map[string]*asyncReq),
		stopCh:     make(chan struct{}),
		options:    options,
	}
}

// Init 初始化集群
func (c *NatsCluster) Init() error {
	kklog.Infof("NatsCluster(%s) startup", c.nodeID)
	return c.connectAndSubscribe()
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

// PublishRemote 发布消息到指定节点
func (c *NatsCluster) PublishRemote(nodeID string, packet *kkcluster.ClusterPacket) error {
	if packet == nil {
		return kkerrors.ErrInvalidPacket
	}

	// 检查目标节点是否存在
	_, found := c.discovery.GetMember(nodeID)
	if !found {
		return kkerrors.ErrMemberNotFound
	}

	// 设置源节点ID
	packet.SourcePath = c.nodeID
	packet.TargetPath = nodeID

	// 序列化消息
	data, err := msgCodec.Marshal(packet)
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

// PublishRemoteType 根据节点类型发布消息
// 优化：只需发布一次到类型主题，所有订阅了该类型主题的节点都会收到消息
// 注意：节点在 Init() 时会订阅自己类型的主题，使用普通 Subscribe（不是 QueueSubscribe）
// 如果需要负载均衡（消息只被一个节点接收），应使用 QueueSubscribe
func (c *NatsCluster) PublishRemoteType(nodeType string, packet *kkcluster.ClusterPacket) error {
	if packet == nil {
		return kkerrors.ErrInvalidPacket
	}

	// 检查该类型是否有节点（可选，用于提前验证）
	if members := c.discovery.ListByType(nodeType); len(members) == 0 {
		return kkerrors.ErrNoMemberOfType
	}

	// 设置源节点ID
	packet.SourcePath = c.nodeID
	packet.TargetPath = nodeType

	// 序列化消息（只序列化一次）
	data, err := msgCodec.Marshal(packet)
	kkcluster.PutClusterPacket(packet)
	if err != nil {
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) marshal type publish packet failed: nodeType=%s err=%v", c.nodeID, nodeType, err)
		return err
	}

	// 优化：使用队列组，只需发布一次到类型主题
	// NATS 会自动将消息分发给订阅了该主题的队列组中的一个节点
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
		return kkerrors.ErrInvalidPacket
	}
	if callback == nil {
		return kkcluster.ErrFromCode(kkcluster.ClusterErrorCodeInvalidRequest)
	}

	_, found := c.discovery.GetMember(nodeID)
	if !found {
		return kkerrors.ErrMemberNotFound
	}

	reqTimeout := 5 * time.Second
	if len(timeout) > 0 && timeout[0] > 0 {
		reqTimeout = timeout[0]
	}
	packet.Timeout = int64(reqTimeout.Milliseconds())

	requestID := c.generateRequestID()

	pending := &asyncReq{cb: callback}
	c.reqMu.Lock()
	c.reqMap[requestID] = pending
	c.reqMu.Unlock()

	reqMsg := &kkcluster.ClusterRequest{
		RequestID:    requestID,
		SourceNodeID: c.nodeID,
		Packet:       packet,
	}

	data, err := msgCodec.Marshal(reqMsg)
	kkcluster.PutClusterPacket(packet)
	if err != nil {
		c.reqMu.Lock()
		delete(c.reqMap, requestID)
		c.reqMu.Unlock()
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) marshal async request failed: requestID=%s targetNode=%s err=%v", c.nodeID, requestID, nodeID, err)
		return err
	}

	requestSubject := c.getRequestSubjectForNode(nodeID)
	if err := c.conn.Publish(requestSubject, data); err != nil {
		c.reqMu.Lock()
		delete(c.reqMap, requestID)
		c.reqMu.Unlock()
		c.stats.AddError()
		kklog.Errorf("NatsCluster(%s) publish async request failed: subject=%s requestID=%s targetNode=%s bytes=%d err=%v", c.nodeID, requestSubject, requestID, nodeID, len(data), err)
		return err
	}

	c.stats.AddRequestSent(len(data))

	go func() {
		time.Sleep(reqTimeout)
		c.reqMu.Lock()
		p := c.reqMap[requestID]
		delete(c.reqMap, requestID)
		c.reqMu.Unlock()
		if p != nil {
			c.stats.AddError()
			cb := p.cb
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
	}()

	return nil
}

// RequestRemote 请求消息（带响应）
func (c *NatsCluster) RequestRemote(nodeID string, packet *kkcluster.ClusterPacket, timeout ...time.Duration) ([]byte, kkcluster.ClusterErrorCode) {
	if packet == nil {
		return nil, kkcluster.ClusterErrorCodeInvalidRequest
	}

	// 检查目标节点是否存在
	_, found := c.discovery.GetMember(nodeID)
	if !found {
		return nil, kkcluster.ClusterErrorCodeMemberNotFound
	}

	// 设置超时
	reqTimeout := 5 * time.Second
	if len(timeout) > 0 && timeout[0] > 0 {
		reqTimeout = timeout[0]
	}
	packet.Timeout = int64(reqTimeout.Milliseconds())

	// 生成请求ID
	requestID := c.generateRequestID()

	// 创建响应通道（不关闭，避免超时后晚到响应向已关闭 channel 发送导致 panic）
	responseCh := make(chan *kkcluster.ClusterResponse, 1)
	c.requestMu.Lock()
	c.requestMap[requestID] = responseCh
	c.requestMu.Unlock()

	// 确保清理
	defer func() {
		c.requestMu.Lock()
		delete(c.requestMap, requestID)
		c.requestMu.Unlock()
	}()

	// 创建请求消息
	reqMsg := &kkcluster.ClusterRequest{
		RequestID:    requestID,
		SourceNodeID: c.nodeID,
		Packet:       packet,
	}

	// 序列化请求
	data, err := msgCodec.Marshal(reqMsg)
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
	kklog.Infof("NatsCluster(%s) shutdown", c.nodeID)
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
	if c.conn != nil {
		c.conn.Close()
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

// Stats 获取统计信息快照
func (c *NatsCluster) Stats() kkcluster.ClusterStatsSnapshot {
	isConnected := c.conn != nil && c.conn.IsConnected()
	return c.stats.Snapshot(isConnected)
}

// handleResponse 处理响应（单订阅分发）
func (c *NatsCluster) handleResponse(msg *nats.Msg) {
	var resp kkcluster.ClusterResponse
	if err := msgCodec.Unmarshal(msg.Data, &resp); err != nil {
		kklog.Errorf("NatsCluster(%s) unmarshal response failed: %v", c.nodeID, err)
		c.stats.AddError()
		return
	}
	if resp.RequestID == "" {
		// 防御：没有 requestID 的响应无法路由
		c.stats.AddError()
		return
	}

	// 1) 优先投递同步请求（如果还在等待）
	c.requestMu.RLock()
	ch := c.requestMap[resp.RequestID]
	c.requestMu.RUnlock()
	if ch != nil {
		select {
		case ch <- &resp:
		default:
		}
		// 同步路径的 AddResponseReceived 在 RequestRemote 等待处统计
		return
	}

	// 2) 投递异步请求（如果还在等待）
	c.reqMu.Lock()
	p := c.reqMap[resp.RequestID]
	delete(c.reqMap, resp.RequestID)
	c.reqMu.Unlock()
	if p == nil || p.cb == nil {
		return
	}

	// 记录接收响应统计（异步路径只有这里能统计）
	c.stats.AddResponseReceived(len(resp.Data))

	data := make([]byte, len(resp.Data))
	copy(data, resp.Data)
	code := kkcluster.ClusterErrorCode(resp.Code)
	cb := p.cb
	xcall.AntsGo(func() {
		defer func() {
			if r := recover(); r != nil {
				c.stats.AddError()
				kklog.Errorf("NatsCluster(%s) RequestRemoteAsync callback panic: %v", c.nodeID, r)
			}
		}()
		cb(data, code)
	})
}

// handleRequest 处理请求
func (c *NatsCluster) handleRequest(msg *nats.Msg) {
	// 记录接收请求统计
	c.stats.AddRequestReceived(len(msg.Data))

	var req kkcluster.ClusterRequest
	if err := msgCodec.Unmarshal(msg.Data, &req); err != nil {
		kklog.Errorf("NatsCluster(%s) unmarshal request failed: %v", c.nodeID, err)
		c.stats.AddError()
		return
	}

	response := &kkcluster.ClusterResponse{
		RequestID: req.RequestID,
		Code:      int32(kkcluster.ClusterErrorCodeFail),
		Data:      nil,
	}
	// 调用用户注册的处理器来处理请求
	if c.requestHandler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.stats.AddError()
					kklog.Errorf("NatsCluster(%s) request handler panic: %v", c.nodeID, r)
				}
			}()
			resp, err := c.requestHandler(&req)
			if err != nil || resp == nil {
				kklog.Errorf("NatsCluster(%s) request handler failed: %v", c.nodeID, err)
				c.stats.AddError()
				return
			}
			response.Code = resp.Code
			response.Data = resp.Data
		}()
	}

	// 发送响应
	responseSubject := c.getResponseSubject(req.RequestID)
	data, err := msgCodec.Marshal(response)
	if err != nil {
		kklog.Errorf("NatsCluster(%s) marshal response failed: %v", c.nodeID, err)
		c.stats.AddError()
		return
	}

	if err := c.conn.Publish(responseSubject, data); err != nil {
		kklog.Errorf("NatsCluster(%s) publish response failed: %v", c.nodeID, err)
		c.stats.AddError()
	} else {
		// 记录发送响应统计
		c.stats.AddResponseSent(len(data))
	}
}

// handlePublish 处理发布消息（来自节点ID主题）
func (c *NatsCluster) handlePublish(msg *nats.Msg) {
	// 记录接收发布消息统计
	c.stats.AddPublishReceived(len(msg.Data))

	var packet kkcluster.ClusterPacket
	if err := msgCodec.Unmarshal(msg.Data, &packet); err != nil {
		kklog.Errorf("NatsCluster(%s) unmarshal publish packet failed: %v", c.nodeID, err)
		c.stats.AddError()
		return
	}

	// 调用用户注册的处理器
	if c.publishHandler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.stats.AddError()
					kklog.Errorf("NatsCluster(%s) publish handler panic: %v", c.nodeID, r)
				}
			}()
			c.publishHandler(packet.SourcePath, &packet)
		}()
	}
}

// handleTypePublish 处理类型发布消息（来自类型主题）
func (c *NatsCluster) handleTypePublish(msg *nats.Msg) {
	// 记录接收发布消息统计
	c.stats.AddPublishReceived(len(msg.Data))

	var packet kkcluster.ClusterPacket
	if err := msgCodec.Unmarshal(msg.Data, &packet); err != nil {
		kklog.Errorf("NatsCluster(%s) unmarshal type publish packet failed: %v", c.nodeID, err)
		c.stats.AddError()
		return
	}

	// 调用用户注册的处理器
	if c.publishHandler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.stats.AddError()
					kklog.Errorf("NatsCluster(%s) type publish handler panic: %v", c.nodeID, r)
				}
			}()
			c.publishHandler(packet.SourcePath, &packet)
		}()
	}
}

// generateRequestID 生成请求ID
func (c *NatsCluster) generateRequestID() string {
	seq := kkcluster.GenRequestID()
	return c.nodeID + "." + strconv.FormatUint(seq, 10)
}

// getPublishSubject 获取发布主题
func (c *NatsCluster) getPublishSubject(nodeID string) string {
	return "kkcluster.publish." + nodeID
}

// getPublishTypeSubject 获取类型发布主题
func (c *NatsCluster) getPublishTypeSubject(nodeType string) string {
	return "kkcluster.publish.type." + nodeType
}

// getRequestSubject 获取自己的请求主题
func (c *NatsCluster) getRequestSubject() string {
	return "kkcluster.request." + c.nodeID
}

// getRequestSubjectForNode 获取指定节点的请求主题
func (c *NatsCluster) getRequestSubjectForNode(nodeID string) string {
	return "kkcluster.request." + nodeID
}

// getResponseSubject 获取响应主题
func (c *NatsCluster) getResponseSubject(requestID string) string {
	return "kkcluster.response." + requestID
}

// getResponseSubjectPattern 返回本节点需要订阅的响应主题（通配）
func (c *NatsCluster) getResponseSubjectPattern() string {
	// requestID = <sourceNodeID>.<seq>，响应主题为 kkcluster.response.<requestID>
	// 仅订阅本节点发起请求的响应：kkcluster.response.<nodeID>.>
	return "kkcluster.response." + c.nodeID + ".>"
}
