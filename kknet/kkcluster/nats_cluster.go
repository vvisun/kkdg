package kkcluster

import (
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/utils/kklog"
)

// NatsCluster 基于NATS的集群实现
type NatsCluster struct {
	nodeID      string
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

	// 发布消息处理器
	publishHandler func(nodeID string, packet *ClusterPacket)

	// 请求处理器
	requestHandler func(req *ClusterRequest) (*ClusterResponse, error)

	stopCh chan struct{}
}

// NewNatsCluster 创建新的NATS集群
func NewNatsCluster(nodeID string, discovery IDiscovery, natsAddress string) *NatsCluster {
	if natsAddress == "" {
		natsAddress = defaultNatsAddress
	}
	return &NatsCluster{
		nodeID:      nodeID,
		discovery:   discovery,
		natsAddress: natsAddress,
		requestMap:  make(map[string]chan *ClusterResponse),
		stopCh:      make(chan struct{}),
	}
}

// Init 初始化集群
func (c *NatsCluster) Init() error {
	// 连接到NATS
	conn, err := nats.Connect(c.natsAddress)
	if err != nil {
		return err
	}
	c.conn = conn

	// 订阅请求主题（用于接收其他节点的请求）
	requestSubject := c.getRequestSubject()
	sub, err := conn.Subscribe(requestSubject, c.handleRequest)
	if err != nil {
		conn.Close()
		return err
	}
	c.requestSub = sub

	// 订阅发布消息主题（用于接收其他节点发送的消息）
	publishSubject := c.getPublishSubject(c.nodeID)
	publishSub, err := conn.Subscribe(publishSubject, c.handlePublish)
	if err != nil {
		sub.Unsubscribe()
		conn.Close()
		return err
	}
	c.publishSub = publishSub

	return nil
}

// PublishRemote 发布消息到指定节点
func (c *NatsCluster) PublishRemote(nodeID string, packet *ClusterPacket) error {
	if packet == nil {
		return ErrInvalidPacket
	}

	// 检查目标节点是否存在
	_, found := c.discovery.GetMember(nodeID)
	if !found {
		return ErrMemberNotFound
	}

	// 设置源节点ID
	packet.SourcePath = c.nodeID
	packet.TargetPath = nodeID

	// 序列化消息
	data, err := json.Marshal(packet)
	if err != nil {
		return err
	}

	// 发布到目标节点的主题
	subject := c.getPublishSubject(nodeID)
	return c.conn.Publish(subject, data)
}

// PublishRemoteType 根据节点类型发布消息
func (c *NatsCluster) PublishRemoteType(nodeType string, packet *ClusterPacket) error {
	if packet == nil {
		return ErrInvalidPacket
	}

	// 获取该类型的所有节点
	members := c.discovery.ListByType(nodeType)
	if len(members) == 0 {
		return ErrNoMemberOfType
	}

	// 设置源节点ID
	packet.SourcePath = c.nodeID
	packet.TargetPath = nodeType

	// 序列化消息
	data, err := json.Marshal(packet)
	if err != nil {
		return err
	}

	// PublishRemoteType 原本发布到类型主题 kkcluster.publish.type.{nodeType}，但节点在 Init() 时只
	// 订阅了自己的节点ID主题 kkcluster.publish.{nodeID}，因此收不到类型主题的消息。
	// 因此，这里改为向每个同类型节点单独发送消息。
	// // 发布到该类型的所有节点
	// subject := c.getPublishTypeSubject(nodeType)
	// return c.conn.Publish(subject, data)

	// 向每个同类型节点单独发送消息
	for _, member := range members {
		// 跳过自己
		if member.GetNodeID() == c.nodeID {
			continue
		}

		subject := c.getPublishSubject(member.GetNodeID())
		if err := c.conn.Publish(subject, data); err != nil {
			kklog.Warnf("NatsCluster publish to %s failed: %v", member.GetNodeID(), err)
			// 继续发送给其他节点，不因为一个节点失败而停止
		}
	}

	return nil
}

// RequestRemote 请求消息（带响应）
func (c *NatsCluster) RequestRemote(nodeID string, packet *ClusterPacket, timeout ...time.Duration) ([]byte, int32) {
	if packet == nil {
		return nil, -1
	}

	// 检查目标节点是否存在
	_, found := c.discovery.GetMember(nodeID)
	if !found {
		return nil, -1
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
	c.requestMu.Lock()
	c.requestMap[requestID] = responseCh
	c.requestMu.Unlock()

	// 确保清理
	defer func() {
		c.requestMu.Lock()
		delete(c.requestMap, requestID)
		close(responseCh)
		c.requestMu.Unlock()
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
		return nil, -1
	}

	// 订阅响应主题
	responseSubject := c.getResponseSubject(requestID)
	responseSub, err := c.conn.Subscribe(responseSubject, func(msg *nats.Msg) {
		var resp ClusterResponse
		if err := json.Unmarshal(msg.Data, &resp); err != nil {
			kklog.Warnf("NatsCluster unmarshal response failed: %v", err)
			return
		}

		select {
		case responseCh <- &resp:
		default:
		}
	})
	if err != nil {
		return nil, -1
	}
	defer func() {
		_ = responseSub.Unsubscribe()
	}()

	// 发布请求到目标节点的请求主题
	requestSubject := c.getRequestSubjectForNode(nodeID)
	if err := c.conn.Publish(requestSubject, data); err != nil {
		return nil, -1
	}

	// 等待响应
	select {
	case resp := <-responseCh:
		return resp.Data, resp.Code
	case <-time.After(reqTimeout):
		return nil, -1
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
	if c.conn != nil {
		c.conn.Close()
	}
}

// handleRequest 处理请求
func (c *NatsCluster) handleRequest(msg *nats.Msg) {
	var req ClusterRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		kklog.Warnf("NatsCluster unmarshal request failed: %v", err)
		return
	}

	response := &ClusterResponse{
		RequestID: req.RequestID,
		Code:      0,
		Data:      nil,
	}
	// 调用用户注册的处理器来处理请求
	if c.requestHandler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					kklog.Errorf("NatsCluster request handler panic: %v", r)
				}
			}()
			resp, err := c.requestHandler(&req)
			if err != nil || resp == nil {
				kklog.Warnf("NatsCluster request handler failed: %v", err)
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
		kklog.Warnf("NatsCluster marshal response failed: %v", err)
		return
	}

	if err := c.conn.Publish(responseSubject, data); err != nil {
		kklog.Warnf("NatsCluster publish response failed: %v", err)
	}
}

// SetRequestHandler 设置请求处理器
func (c *NatsCluster) SetRequestHandler(handler func(req *ClusterRequest) (*ClusterResponse, error)) {
	c.requestHandler = handler
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

// handlePublish 处理发布消息
func (c *NatsCluster) handlePublish(msg *nats.Msg) {
	var packet ClusterPacket
	if err := json.Unmarshal(msg.Data, &packet); err != nil {
		kklog.Warnf("NatsCluster unmarshal publish packet failed: %v", err)
		return
	}

	// 调用用户注册的处理器
	if c.publishHandler != nil {
		func() {
			defer func() {
				if r := recover(); r != nil {
					kklog.Errorf("NatsCluster publish handler panic: %v", r)
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

// generateRequestID 生成请求ID
func (c *NatsCluster) generateRequestID() string {
	seq := atomic.AddUint64(&c.requestSeq, 1)
	return c.nodeID + "." + strconv.FormatUint(seq, 10)
}

// ClusterRequest 集群请求消息
type ClusterRequest struct {
	RequestID    string         `json:"requestID"`
	SourceNodeID string         `json:"sourceNodeID"`
	Packet       *ClusterPacket `json:"packet"`
}

// ClusterResponse 集群响应消息
type ClusterResponse struct {
	RequestID string `json:"requestID"`
	Code      int32  `json:"code"`
	Data      []byte `json:"data"`
}
