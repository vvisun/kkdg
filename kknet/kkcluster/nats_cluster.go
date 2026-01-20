package kkcluster

import (
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/utils/kklog"
)

// NatsCluster 基于NATS的集群实现
type NatsCluster struct {
	nodeID      string
	nodeType    string // 节点类型（用于订阅类型主题）
	discovery   IDiscovery
	natsAddress string
	conn        *nats.Conn

	// 请求响应处理
	requestSub *nats.Subscription
	requestMap map[string]chan *ClusterResponse
	requestMu  sync.RWMutex
	requestSeq uint64

	// 发布消息订阅
	publishSub *nats.Subscription

	// 类型发布消息订阅（使用队列组）
	typePublishSub *nats.Subscription

	// 发布消息处理器
	publishHandler func(nodeID string, packet *ClusterPacket)

	// 请求处理器
	requestHandler func(req *ClusterRequest) (*ClusterResponse, error)

	// 订阅管理（用于重连时重新订阅）
	subMu sync.Mutex

	// 统计信息
	stats ClusterStats

	stopCh chan struct{}

	options []nats.Option
}

// NewNatsCluster 创建新的NATS集群
func NewNatsCluster(nodeID string, discovery IDiscovery, natsAddress string, options ...nats.Option) *NatsCluster {
	if natsAddress == "" {
		natsAddress = defaultNatsAddress
	}

	// 尝试从 discovery 获取节点类型（如果是 NatsDiscovery）
	var nodeType string
	if nd, ok := discovery.(*NatsDiscovery); ok {
		nodeType = nd.nodeType
	}

	return &NatsCluster{
		nodeID:      nodeID,
		nodeType:    nodeType,
		discovery:   discovery,
		natsAddress: natsAddress,
		requestMap:  make(map[string]chan *ClusterResponse),
		stopCh:      make(chan struct{}),
		options:     options,
	}
}

// Init 初始化集群
func (c *NatsCluster) Init() error {
	return c.connectAndSubscribe()
}

// connectAndSubscribe 连接NATS并订阅主题
func (c *NatsCluster) connectAndSubscribe() error {
	// 配置NATS连接选项，启用自动重连
	opts := nats.GetDefaultOptions()
	for _, option := range c.options {
		option(&opts)
	}

	// 设置重连处理器
	opts.ReconnectedCB = func(nc *nats.Conn) {
		kklog.Infof("NatsCluster reconnected to %s", nc.ConnectedUrl())
		c.stats.AddReconnect()
		// 重连后重新订阅
		if err := c.resubscribe(); err != nil {
			kklog.Errorf("NatsCluster resubscribe failed: %v", err)
			c.stats.AddError()
		}
	}

	// 设置断开连接处理器
	opts.DisconnectedErrCB = func(nc *nats.Conn, err error) {
		if err != nil {
			kklog.Warnf("NatsCluster disconnected: %v", err)
		} else {
			kklog.Warnf("NatsCluster disconnected")
		}
	}

	// 设置关闭处理器
	opts.ClosedCB = func(nc *nats.Conn) {
		kklog.Infof("NatsCluster connection closed")
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
		sub.Unsubscribe()
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
			sub.Unsubscribe()
			publishSub.Unsubscribe()
			return err
		}
		c.typePublishSub = typePublishSub
	}

	return nil
}

// PublishRemote 发布消息到指定节点
func (c *NatsCluster) PublishRemote(nodeID string, packet *ClusterPacket) error {
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
	data, err := json.Marshal(packet)
	if err != nil {
		c.stats.AddError()
		return err
	}

	// 发布到目标节点的主题
	subject := c.getPublishSubject(nodeID)
	if err := c.conn.Publish(subject, data); err != nil {
		c.stats.AddError()
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
func (c *NatsCluster) PublishRemoteType(nodeType string, packet *ClusterPacket) error {
	if packet == nil {
		return kkerrors.ErrInvalidPacket
	}

	// 检查该类型是否有节点（可选，用于提前验证）
	members := c.discovery.ListByType(nodeType)
	if len(members) == 0 {
		return kkerrors.ErrNoMemberOfType
	}

	// 设置源节点ID
	packet.SourcePath = c.nodeID
	packet.TargetPath = nodeType

	// 序列化消息（只序列化一次）
	data, err := json.Marshal(packet)
	if err != nil {
		c.stats.AddError()
		return err
	}

	// 优化：使用队列组，只需发布一次到类型主题
	// NATS 会自动将消息分发给订阅了该主题的队列组中的一个节点
	subject := c.getPublishTypeSubject(nodeType)
	if err := c.conn.Publish(subject, data); err != nil {
		c.stats.AddError()
		return err
	}

	// 记录统计
	c.stats.AddPublishSent(len(data))
	return nil
}

// RequestRemote 请求消息（带响应）
func (c *NatsCluster) RequestRemote(nodeID string, packet *ClusterPacket, timeout ...time.Duration) ([]byte, ClusterErrorCode) {
	if packet == nil {
		return nil, ClusterErrorCodeInvalidRequest
	}

	// 检查目标节点是否存在
	_, found := c.discovery.GetMember(nodeID)
	if !found {
		return nil, ClusterErrorCodeMemberNotFound
	}

	// 设置超时
	reqTimeout := 5 * time.Second
	if len(timeout) > 0 && timeout[0] > 0 {
		reqTimeout = timeout[0]
	}

	// 生成请求ID
	requestID := c.generateRequestID()

	// 创建响应通道
	responseCh := make(chan *ClusterResponse, 1)
	var closeOnce sync.Once
	c.requestMu.Lock()
	c.requestMap[requestID] = responseCh
	c.requestMu.Unlock()

	// 确保清理
	defer func() {
		c.requestMu.Lock()
		delete(c.requestMap, requestID)
		c.requestMu.Unlock()
		// 使用 sync.Once 确保 channel 只关闭一次，避免重复关闭导致 panic
		closeOnce.Do(func() {
			close(responseCh)
		})
	}()

	// 创建请求消息
	reqMsg := &ClusterRequest{
		RequestID:    requestID,
		SourceNodeID: c.nodeID,
		Packet:       packet,
	}

	// 序列化请求
	data, err := json.Marshal(reqMsg)
	if err != nil {
		c.stats.AddError()
		return nil, ClusterErrorCodeMarshalFailed
	}

	// 订阅响应主题
	responseSubject := c.getResponseSubject(requestID)
	responseSub, err := c.conn.Subscribe(responseSubject, func(msg *nats.Msg) {
		var resp ClusterResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			kklog.Errorf("NatsCluster unmarshal response failed: %v", err)
			return
		}

		select {
		case responseCh <- &resp:
		default:
		}
	})
	if err != nil {
		return nil, ClusterErrorCodeSubscribeFailed
	}
	defer func() {
		_ = responseSub.Unsubscribe()
	}()

	// 发布请求到目标节点的请求主题
	requestSubject := c.getRequestSubjectForNode(nodeID)
	if err := c.conn.Publish(requestSubject, data); err != nil {
		c.stats.AddError()
		return nil, ClusterErrorCodePublishFailed
	}

	// 记录发送请求统计
	c.stats.AddRequestSent(len(data))

	// 等待响应
	select {
	case resp := <-responseCh:
		// 记录接收响应统计
		c.stats.AddResponseReceived(len(resp.Data))
		return resp.Data, ClusterErrorCode(resp.Code)
	case <-time.After(reqTimeout):
		c.stats.AddError()
		return nil, ClusterErrorCodeTimeout
	}
}

// Stop 停止集群
func (c *NatsCluster) Stop() {
	select {
	case <-c.stopCh:
		return
	default:
		close(c.stopCh)
	}

	if c.requestSub != nil {
		_ = c.requestSub.Unsubscribe()
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

// handleRequest 处理请求
func (c *NatsCluster) handleRequest(msg *nats.Msg) {
	// 记录接收请求统计
	c.stats.AddRequestReceived(len(msg.Data))

	var req ClusterRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		kklog.Errorf("NatsCluster unmarshal request failed: %v", err)
		c.stats.AddError()
		return
	}

	response := &ClusterResponse{
		RequestID: req.RequestID,
		Code:      int32(ClusterErrorCodeFail),
		Data:      nil,
	}
	// 调用用户注册的处理器来处理请求
	if c.requestHandler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.stats.AddError()
					kklog.Errorf("NatsCluster request handler panic: %v", r)
				}
			}()
			resp, err := c.requestHandler(&req)
			if err != nil || resp == nil {
				kklog.Errorf("NatsCluster request handler failed: %v", err)
				c.stats.AddError()
				return
			}
			response.Code = resp.Code
			response.Data = resp.Data
		}()
	}

	// 发送响应
	responseSubject := c.getResponseSubject(req.RequestID)
	data, err := json.Marshal(response)
	if err != nil {
		kklog.Errorf("NatsCluster marshal response failed: %v", err)
		c.stats.AddError()
		return
	}

	if err := c.conn.Publish(responseSubject, data); err != nil {
		kklog.Errorf("NatsCluster publish response failed: %v", err)
		c.stats.AddError()
	} else {
		// 记录发送响应统计
		c.stats.AddResponseSent(len(data))
	}
}

// SetRequestHandler 设置请求处理器
func (c *NatsCluster) SetRequestHandler(handler func(req *ClusterRequest) (*ClusterResponse, error)) {
	c.requestHandler = handler
}

// handlePublish 处理发布消息（来自节点ID主题）
func (c *NatsCluster) handlePublish(msg *nats.Msg) {
	// 记录接收发布消息统计
	c.stats.AddPublishReceived(len(msg.Data))

	var packet ClusterPacket
	if err := json.Unmarshal(msg.Data, &packet); err != nil {
		kklog.Errorf("NatsCluster unmarshal publish packet failed: %v", err)
		c.stats.AddError()
		return
	}

	// 调用用户注册的处理器
	if c.publishHandler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.stats.AddError()
					kklog.Errorf("NatsCluster publish handler panic: %v", r)
				}
			}()
			c.publishHandler(packet.SourcePath, &packet)
		}()
	}
}

// handleTypePublish 处理类型发布消息（来自类型主题，使用队列组）
func (c *NatsCluster) handleTypePublish(msg *nats.Msg) {
	// 记录接收发布消息统计
	c.stats.AddPublishReceived(len(msg.Data))

	var packet ClusterPacket
	if err := json.Unmarshal(msg.Data, &packet); err != nil {
		kklog.Errorf("NatsCluster unmarshal type publish packet failed: %v", err)
		c.stats.AddError()
		return
	}

	// 调用用户注册的处理器
	if c.publishHandler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					c.stats.AddError()
					kklog.Errorf("NatsCluster type publish handler panic: %v", r)
				}
			}()
			c.publishHandler(packet.SourcePath, &packet)
		}()
	}
}

// SetPublishHandler 设置发布消息处理器
func (c *NatsCluster) SetPublishHandler(handler func(nodeID string, packet *ClusterPacket)) {
	c.publishHandler = handler
}

// Stats 获取统计信息快照
func (c *NatsCluster) Stats() ClusterStatsSnapshot {
	isConnected := c.conn != nil && c.conn.IsConnected()
	return c.stats.Snapshot(isConnected)
}

// generateRequestID 生成请求ID
func (c *NatsCluster) generateRequestID() string {
	seq := atomic.AddUint64(&c.requestSeq, 1)
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
