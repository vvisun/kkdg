package kkcluster

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/utils/kklog"
)

// NatsDiscovery 基于NATS的服务发现实现
type NatsDiscovery struct {
	name        string
	nodeID      string
	nodeType    string
	address     string
	settings    map[string]string
	natsAddress string
	conn        *nats.Conn
	sub         *nats.Subscription
	requestSub  *nats.Subscription

	members         map[string]IMember
	memberTimes     map[string]time.Time // 记录成员最后更新时间
	membersMu       sync.RWMutex
	addListeners    []MemberListener
	removeListeners []MemberListener
	listenersMu     sync.RWMutex

	stopCh chan struct{}
	doneCh chan struct{}
}

var _ IDiscovery = (*NatsDiscovery)(nil)

// NewNatsDiscovery 创建新的NATS服务发现
func NewNatsDiscovery(name, nodeID, nodeType, address, natsAddress string, settings map[string]string) *NatsDiscovery {
	if natsAddress == "" {
		natsAddress = defaultNatsAddress
	}
	if settings == nil {
		settings = make(map[string]string)
	}
	return &NatsDiscovery{
		name:        name,
		nodeID:      nodeID,
		nodeType:    nodeType,
		address:     address,
		settings:    settings,
		natsAddress: natsAddress,
		members:     make(map[string]IMember),
		memberTimes: make(map[string]time.Time),
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
	}
}

// Name 返回发现服务名称
func (d *NatsDiscovery) Name() string {
	return d.name
}

// Map 获取成员列表
func (d *NatsDiscovery) Map() map[string]IMember {
	d.membersMu.RLock()
	defer d.membersMu.RUnlock()

	result := make(map[string]IMember, len(d.members))
	for k, v := range d.members {
		result[k] = v
	}
	return result
}

// ListByType 根据节点类型获取列表
func (d *NatsDiscovery) ListByType(nodeType string, filterNodeID ...string) []IMember {
	d.membersMu.RLock()
	defer d.membersMu.RUnlock()

	var result []IMember
	filterMap := make(map[string]bool)
	for _, id := range filterNodeID {
		filterMap[id] = true
	}

	for _, member := range d.members {
		if member.GetNodeType() == nodeType {
			if len(filterMap) == 0 || !filterMap[member.GetNodeID()] {
				result = append(result, member)
			}
		}
	}
	return result
}

// Random 根据节点类型随机一个
func (d *NatsDiscovery) Random(nodeType string) (IMember, bool) {
	list := d.ListByType(nodeType)
	if len(list) == 0 {
		return nil, false
	}
	// 简单的随机选择（可以使用更复杂的随机算法）
	return list[0], true
}

// GetType 根据节点id获取类型
func (d *NatsDiscovery) GetType(nodeID string) (string, error) {
	d.membersMu.RLock()
	defer d.membersMu.RUnlock()

	member, found := d.members[nodeID]
	if !found {
		return "", ErrMemberNotFound
	}
	return member.GetNodeType(), nil
}

// GetMember 获取成员
func (d *NatsDiscovery) GetMember(nodeID string) (IMember, bool) {
	d.membersMu.RLock()
	defer d.membersMu.RUnlock()

	member, found := d.members[nodeID]
	return member, found
}

// AddMember 添加成员
func (d *NatsDiscovery) AddMember(member IMember) {
	if member == nil {
		return
	}

	d.membersMu.Lock()
	_, existed := d.members[member.GetNodeID()]
	d.members[member.GetNodeID()] = member
	d.memberTimes[member.GetNodeID()] = time.Now()
	d.membersMu.Unlock()

	if !existed {
		d.notifyAddListeners(member)
	}
}

// RemoveMember 移除成员
func (d *NatsDiscovery) RemoveMember(nodeID string) {
	d.membersMu.Lock()
	member, existed := d.members[nodeID]
	if existed {
		delete(d.members, nodeID)
		delete(d.memberTimes, nodeID)
	}
	d.membersMu.Unlock()

	if existed {
		d.notifyRemoveListeners(member)
	}
}

// OnAddMember 添加成员监听函数
func (d *NatsDiscovery) OnAddMember(listener MemberListener) {
	if listener == nil {
		return
	}
	d.listenersMu.Lock()
	d.addListeners = append(d.addListeners, listener)
	d.listenersMu.Unlock()
}

// OnRemoveMember 移除成员监听函数
func (d *NatsDiscovery) OnRemoveMember(listener MemberListener) {
	if listener == nil {
		return
	}
	d.listenersMu.Lock()
	d.removeListeners = append(d.removeListeners, listener)
	d.listenersMu.Unlock()
}

// Stop 停止服务发现
func (d *NatsDiscovery) Stop() {
	select {
	case <-d.stopCh:
		return
	default:
		close(d.stopCh)
	}

	if d.sub != nil {
		_ = d.sub.Unsubscribe()
	}
	if d.requestSub != nil {
		_ = d.requestSub.Unsubscribe()
	}
	if d.conn != nil {
		d.conn.Close()
	}

	close(d.doneCh)
}

// Start 启动服务发现（需要在外部调用）
func (d *NatsDiscovery) Start() error {
	// 连接到NATS
	conn, err := nats.Connect(d.natsAddress)
	if err != nil {
		return err
	}
	d.conn = conn

	// 订阅服务发现主题
	subject := d.getDiscoverySubject()
	sub, err := conn.Subscribe(subject, d.handleDiscoveryMessage)
	if err != nil {
		conn.Close()
		return err
	}
	d.sub = sub

	// 订阅服务发现请求主题（用于响应其他节点的请求）
	requestSubject := d.getDiscoveryRequestSubject()
	requestSub, err := conn.Subscribe(requestSubject, d.handleDiscoveryRequest)
	if err != nil {
		sub.Unsubscribe()
		conn.Close()
		return err
	}
	d.requestSub = requestSub

	// 发布自己的信息
	if err := d.publishSelf(); err != nil {
		kklog.Warnf("NatsDiscovery publish self failed: %v", err)
	}

	// 启动心跳
	go d.heartbeatLoop()

	// 启动请求所有成员
	go d.requestAllMembers()

	// 启动成员超时检查
	go d.checkMemberTimeout()

	return nil
}

// getDiscoverySubject 获取服务发现主题
func (d *NatsDiscovery) getDiscoverySubject() string {
	return "kkcluster.discovery"
}

// handleDiscoveryMessage 处理服务发现消息
func (d *NatsDiscovery) handleDiscoveryMessage(msg *nats.Msg) {
	var memberInfo MemberInfo
	if err := json.Unmarshal(msg.Data, &memberInfo); err != nil {
		kklog.Warnf("NatsDiscovery unmarshal member info failed: %v", err)
		return
	}

	// 忽略自己
	if memberInfo.NodeID == d.nodeID {
		return
	}

	member := NewMember(
		memberInfo.NodeID,
		memberInfo.NodeType,
		memberInfo.Address,
		memberInfo.Settings,
	)

	// 更新成员时间
	d.membersMu.Lock()
	if _, existed := d.members[memberInfo.NodeID]; !existed {
		d.membersMu.Unlock()
		d.AddMember(member)
	} else {
		// 只更新时间，不触发通知
		d.memberTimes[memberInfo.NodeID] = time.Now()
		d.membersMu.Unlock()
	}
}

// publishSelf 发布自己的信息
func (d *NatsDiscovery) publishSelf() error {
	memberInfo := MemberInfo{
		NodeID:   d.nodeID,
		NodeType: d.nodeType,
		Address:  d.address,
		Settings: d.settings,
	}

	data, err := json.Marshal(memberInfo)
	if err != nil {
		return err
	}

	return d.conn.Publish(d.getDiscoverySubject(), data)
}

// heartbeatLoop 心跳循环
func (d *NatsDiscovery) heartbeatLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			if err := d.publishSelf(); err != nil {
				kklog.Warnf("NatsDiscovery heartbeat failed: %v", err)
			}
		}
	}
}

// requestAllMembers 请求所有成员
func (d *NatsDiscovery) requestAllMembers() {
	// 延迟一下，等待连接稳定
	time.Sleep(1 * time.Second)

	// 发送请求消息
	reqMsg := &DiscoveryRequest{
		RequesterID: d.nodeID,
	}

	data, err := json.Marshal(reqMsg)
	if err != nil {
		kklog.Warnf("NatsDiscovery marshal request failed: %v", err)
		return
	}

	subject := d.getDiscoveryRequestSubject()
	if err := d.conn.Publish(subject, data); err != nil {
		kklog.Warnf("NatsDiscovery publish request failed: %v", err)
	}
}

// handleDiscoveryRequest 处理服务发现请求
func (d *NatsDiscovery) handleDiscoveryRequest(msg *nats.Msg) {
	var req DiscoveryRequest
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		kklog.Warnf("NatsDiscovery unmarshal request failed: %v", err)
		return
	}

	// 忽略自己的请求
	if req.RequesterID == d.nodeID {
		return
	}

	// 响应自己的信息
	if err := d.publishSelf(); err != nil {
		kklog.Warnf("NatsDiscovery respond to request failed: %v", err)
	}
}

// checkMemberTimeout 检查成员超时
func (d *NatsDiscovery) checkMemberTimeout() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	timeout := 15 * time.Second // 成员超时时间

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			var toRemove []string

			d.membersMu.RLock()
			for nodeID, lastTime := range d.memberTimes {
				if now.Sub(lastTime) > timeout {
					toRemove = append(toRemove, nodeID)
				}
			}
			d.membersMu.RUnlock()

			for _, nodeID := range toRemove {
				kklog.Warnf("NatsDiscovery member %s timeout, removing", nodeID)
				d.RemoveMember(nodeID)
			}
		}
	}
}

// getDiscoveryRequestSubject 获取服务发现请求主题
func (d *NatsDiscovery) getDiscoveryRequestSubject() string {
	return "kkcluster.discovery.request"
}

// notifyAddListeners 通知添加监听器
func (d *NatsDiscovery) notifyAddListeners(member IMember) {
	d.listenersMu.RLock()
	listeners := make([]MemberListener, len(d.addListeners))
	copy(listeners, d.addListeners)
	d.listenersMu.RUnlock()

	for _, listener := range listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					kklog.Errorf("NatsDiscovery add listener panic: %v", r)
				}
			}()
			listener(member)
		}()
	}
}

// notifyRemoveListeners 通知移除监听器
func (d *NatsDiscovery) notifyRemoveListeners(member IMember) {
	d.listenersMu.RLock()
	listeners := make([]MemberListener, len(d.removeListeners))
	copy(listeners, d.removeListeners)
	d.listenersMu.RUnlock()

	for _, listener := range listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					kklog.Errorf("NatsDiscovery remove listener panic: %v", r)
				}
			}()
			listener(member)
		}()
	}
}

// MemberInfo 成员信息（用于序列化）
type MemberInfo struct {
	NodeID   string            `json:"nodeID"`
	NodeType string            `json:"nodeType"`
	Address  string            `json:"address"`
	Settings map[string]string `json:"settings"`
}

// DiscoveryRequest 发现请求
type DiscoveryRequest struct {
	RequesterID string `json:"requesterID"`
}
