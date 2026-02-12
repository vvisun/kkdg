package dnats

import (
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kklog"
	"github.com/vvisun/kkdg/utils/xrand"
)

// NatsDiscovery 基于NATS的服务发现实现
type NatsDiscovery struct {
	name       string
	nodeID     string
	nodeType   string
	address    string
	settings   map[string]string
	conn       *nats.Conn
	sub        *nats.Subscription
	requestSub *nats.Subscription

	members         map[string]kkdiscovery.IMember // key: nodeID, value: member
	memberTimes     map[string]time.Time           // 记录成员最后更新时间。key: nodeID, value: last update time
	membersMu       sync.RWMutex
	addListeners    []kkdiscovery.MemberListener
	removeListeners []kkdiscovery.MemberListener
	listenersMu     sync.RWMutex

	// 订阅管理（用于重连时重新订阅）
	subMu sync.Mutex

	// 启动标志（确保goroutine只启动一次）
	startedOnce sync.Once

	// 统计信息
	stats kkdiscovery.DiscoveryStats

	stopCh chan struct{}
	doneCh chan struct{}

	options nats.Options
}

var _ kkdiscovery.IDiscovery = (*NatsDiscovery)(nil)

// NewNatsDiscovery 创建新的NATS服务发现
func NewNatsDiscovery(name string, nodeInfo *kkapp.NodeInfo, settings map[string]string, options ...nats.Option) kkdiscovery.IDiscovery {
	if settings == nil {
		settings = make(map[string]string)
	}
	opts := applyNatsOptions(options...)
	return &NatsDiscovery{
		name:        name,
		nodeID:      nodeInfo.GetNodeId(),
		nodeType:    nodeInfo.GetNodeType(),
		address:     nodeInfo.GetAddress(),
		settings:    settings,
		members:     make(map[string]kkdiscovery.IMember), // key: nodeID, value: member
		memberTimes: make(map[string]time.Time),           // key: nodeID, value: last update time
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		options:     opts,
	}
}

// Name 返回发现服务名称
func (d *NatsDiscovery) Name() string {
	return d.name
}

// Map 获取成员列表
func (d *NatsDiscovery) Map() map[string]kkdiscovery.IMember {
	d.membersMu.RLock()
	defer d.membersMu.RUnlock()

	result := make(map[string]kkdiscovery.IMember, len(d.members))
	for k, v := range d.members {
		result[k] = v
	}
	return result
}

// ListByType 根据节点类型获取列表
func (d *NatsDiscovery) ListByType(nodeType string, filterNodeID ...string) []kkdiscovery.IMember {
	d.membersMu.RLock()
	defer d.membersMu.RUnlock()

	var result []kkdiscovery.IMember
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
func (d *NatsDiscovery) Random(nodeType string) (kkdiscovery.IMember, bool) {
	list := d.ListByType(nodeType)
	if len(list) == 0 {
		return nil, false
	}
	idx := xrand.Int(0, len(list)-1)
	return list[idx], true
}

// GetType 根据节点id获取类型
func (d *NatsDiscovery) GetType(nodeID string) (string, error) {
	d.membersMu.RLock()
	defer d.membersMu.RUnlock()

	member, found := d.members[nodeID]
	if !found {
		return "", kkerrors.ErrMemberNotFound
	}
	return member.GetNodeType(), nil
}

// GetMember 获取成员
func (d *NatsDiscovery) GetMember(nodeID string) (kkdiscovery.IMember, bool) {
	d.membersMu.RLock()
	defer d.membersMu.RUnlock()

	member, found := d.members[nodeID]
	return member, found
}

// addMember 添加成员
func (d *NatsDiscovery) addMember(member kkdiscovery.IMember) {
	if member == nil {
		return
	}

	d.membersMu.Lock()
	_, existed := d.members[member.GetNodeID()]
	d.members[member.GetNodeID()] = member
	d.memberTimes[member.GetNodeID()] = time.Now()
	d.membersMu.Unlock()

	if !existed {
		d.stats.AddMember()
		d.notifyAddListeners(member)
	}
}

// removeMember 移除成员
func (d *NatsDiscovery) removeMember(nodeID string) {
	d.membersMu.Lock()
	member, existed := d.members[nodeID]
	if existed {
		delete(d.members, nodeID)
		delete(d.memberTimes, nodeID)
	}
	d.membersMu.Unlock()

	if existed {
		d.stats.RemoveMember()
		d.notifyRemoveListeners(member)
	}
}

// OnAddMember 添加成员监听函数
func (d *NatsDiscovery) OnAddMember(listener kkdiscovery.MemberListener) {
	if listener == nil {
		return
	}
	d.listenersMu.Lock()
	d.addListeners = append(d.addListeners, listener)
	d.listenersMu.Unlock()
}

// OnRemoveMember 移除成员监听函数
func (d *NatsDiscovery) OnRemoveMember(listener kkdiscovery.MemberListener) {
	if listener == nil {
		return
	}
	d.listenersMu.Lock()
	d.removeListeners = append(d.removeListeners, listener)
	d.listenersMu.Unlock()
}

// Stats 获取统计信息快照
func (d *NatsDiscovery) Stats() kkdiscovery.DiscoveryStatsSnapshot {
	d.membersMu.RLock()
	memberCount := len(d.members)
	d.membersMu.RUnlock()

	isConnected := d.conn != nil && d.conn.IsConnected()
	return d.stats.Snapshot(memberCount, isConnected)
}

// Stop 停止服务发现
func (d *NatsDiscovery) Stop() error {
	select {
	case <-d.stopCh:
		return nil
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
	return nil
}

// Start 启动服务发现（需要在外部调用）
func (d *NatsDiscovery) Start() error {
	return d.connectAndSubscribe()
}

// connectAndSubscribe 连接NATS并订阅主题
func (d *NatsDiscovery) connectAndSubscribe() error {
	// 配置NATS连接选项，启用自动重连
	opts := &d.options

	// 设置重连处理器
	opts.ReconnectedCB = func(nc *nats.Conn) {
		kklog.Infof("NatsDiscovery reconnected to %s", nc.ConnectedUrl())
		d.stats.AddReconnect()
		// 重连后重新订阅
		if err := d.resubscribe(); err != nil {
			kklog.Errorf("NatsDiscovery resubscribe failed: %v", err)
			d.stats.AddError()
		}
		// 重连后立即发布自己的信息
		if err := d.publishSelf(); err != nil {
			kklog.Warnf("NatsDiscovery publish self after reconnect failed: %v", err)
			d.stats.AddError()
		}
	}

	// 设置断开连接处理器
	opts.DisconnectedErrCB = func(nc *nats.Conn, err error) {
		if err != nil {
			kklog.Warnf("NatsDiscovery disconnected: %v", err)
		} else {
			kklog.Warnf("NatsDiscovery disconnected")
		}
	}

	// 设置关闭处理器
	opts.ClosedCB = func(nc *nats.Conn) {
		kklog.Infof("NatsDiscovery connection closed")
	}

	// 连接到NATS
	conn, err := opts.Connect()
	if err != nil {
		return err
	}
	d.conn = conn

	// 订阅主题
	if err := d.resubscribe(); err != nil {
		conn.Close()
		return err
	}

	// 发布自己的信息
	if err := d.publishSelf(); err != nil {
		kklog.Warnf("NatsDiscovery publish self failed: %v", err)
	}

	// 启动心跳循环（只启动一次）
	d.startedOnce.Do(func() {
		// 启动心跳
		go d.heartbeatLoop()

		// 启动请求所有成员
		go d.requestAllMembers()

		// 启动成员超时检查
		go d.checkMemberTimeout()
	})

	return nil
}

// resubscribe 重新订阅所有主题
func (d *NatsDiscovery) resubscribe() error {
	d.subMu.Lock()
	defer d.subMu.Unlock()

	if d.conn == nil || !d.conn.IsConnected() {
		return nil
	}

	// 取消旧的订阅
	if d.sub != nil {
		_ = d.sub.Unsubscribe()
		d.sub = nil
	}
	if d.requestSub != nil {
		_ = d.requestSub.Unsubscribe()
		d.requestSub = nil
	}

	// 订阅服务发现主题
	subject := d.getDiscoverySubject()
	sub, err := d.conn.Subscribe(subject, d.handleDiscoveryMessage)
	if err != nil {
		return err
	}
	d.sub = sub

	// 订阅服务发现请求主题（用于响应其他节点的请求）
	requestSubject := d.getDiscoveryRequestSubject()
	requestSub, err := d.conn.Subscribe(requestSubject, d.handleDiscoveryRequest)
	if err != nil {
		sub.Unsubscribe()
		return err
	}
	d.requestSub = requestSub

	return nil
}

// handleDiscoveryMessage 处理服务发现消息
func (d *NatsDiscovery) handleDiscoveryMessage(msg *nats.Msg) {
	// 记录心跳接收统计
	d.stats.AddHeartbeatReceived()

	var memberInfo kkdiscovery.MemberInfo
	if err := msgCodec.Unmarshal(msg.Data, &memberInfo); err != nil {
		kklog.Errorf("NatsDiscovery unmarshal member info failed: %v", err)
		d.stats.AddError()
		return
	}

	// 忽略自己
	if memberInfo.NodeID == d.nodeID {
		return
	}

	member := kkdiscovery.NewMember(
		memberInfo.NodeID,
		memberInfo.NodeType,
		memberInfo.Address,
		memberInfo.Settings,
	)

	// 更新成员时间
	d.membersMu.Lock()
	if _, existed := d.members[memberInfo.NodeID]; !existed {
		d.membersMu.Unlock()
		d.addMember(member)
	} else {
		// 更新成员信息（地址/配置可能变更）及时间，不触发 Add/Remove 通知
		d.members[memberInfo.NodeID] = member
		d.memberTimes[memberInfo.NodeID] = time.Now()
		d.membersMu.Unlock()
	}
}

// publishSelf 发布自己的信息
func (d *NatsDiscovery) publishSelf() error {
	memberInfo := kkdiscovery.MemberInfo{
		NodeID:   d.nodeID,
		NodeType: d.nodeType,
		Address:  d.address,
		Settings: d.settings,
	}

	data, err := msgCodec.Marshal(&memberInfo)
	if err != nil {
		d.stats.AddError()
		return err
	}

	if err := d.conn.Publish(d.getDiscoverySubject(), data); err != nil {
		d.stats.AddError()
		return err
	}

	// 记录心跳发送统计
	d.stats.AddHeartbeatSent()
	return nil
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
				d.stats.AddError()
				kklog.Errorf("NatsDiscovery heartbeat failed: %v", err)
			}
		}
	}
}

// requestAllMembers 请求所有成员
func (d *NatsDiscovery) requestAllMembers() {
	// 延迟一下，等待连接稳定
	time.Sleep(1 * time.Second)

	// 发送请求消息
	reqMsg := kkdiscovery.DiscoveryRequest{
		RequesterID: d.nodeID,
	}

	data, err := msgCodec.Marshal(&reqMsg)
	if err != nil {
		kklog.Errorf("NatsDiscovery marshal request failed: %v", err)
		d.stats.AddError()
		return
	}

	subject := d.getDiscoveryRequestSubject()
	if err := d.conn.Publish(subject, data); err != nil {
		kklog.Errorf("NatsDiscovery publish request failed: %v", err)
		d.stats.AddError()
	}
}

// handleDiscoveryRequest 处理服务发现请求
func (d *NatsDiscovery) handleDiscoveryRequest(msg *nats.Msg) {
	var req kkdiscovery.DiscoveryRequest
	if err := msgCodec.Unmarshal(msg.Data, &req); err != nil {
		kklog.Errorf("NatsDiscovery unmarshal request failed: %v", err)
		d.stats.AddError()
		return
	}

	// 忽略自己的请求
	if req.RequesterID == d.nodeID {
		return
	}

	// 响应自己的信息
	if err := d.publishSelf(); err != nil {
		kklog.Errorf("NatsDiscovery respond to request failed: %v", err)
		// publishSelf内部已经记录了错误统计
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
				d.removeMember(nodeID)
			}
		}
	}
}

// notifyAddListeners 通知添加监听器
func (d *NatsDiscovery) notifyAddListeners(member kkdiscovery.IMember) {
	d.listenersMu.RLock()
	listeners := make([]kkdiscovery.MemberListener, len(d.addListeners))
	copy(listeners, d.addListeners)
	d.listenersMu.RUnlock()

	for _, listener := range listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					d.stats.AddError()
					kklog.Errorf("NatsDiscovery add listener panic: %v", r)
				}
			}()
			listener(member)
		}()
	}
}

// notifyRemoveListeners 通知移除监听器
func (d *NatsDiscovery) notifyRemoveListeners(member kkdiscovery.IMember) {
	d.listenersMu.RLock()
	listeners := make([]kkdiscovery.MemberListener, len(d.removeListeners))
	copy(listeners, d.removeListeners)
	d.listenersMu.RUnlock()

	for _, listener := range listeners {
		func() {
			defer func() {
				if r := recover(); r != nil {
					d.stats.AddError()
					kklog.Errorf("NatsDiscovery remove listener panic: %v", r)
				}
			}()
			listener(member)
		}()
	}
}

// getDiscoverySubject 获取服务发现主题
func (d *NatsDiscovery) getDiscoverySubject() string {
	return "kkcluster.discovery"
}

// getDiscoveryRequestSubject 获取服务发现请求主题
func (d *NatsDiscovery) getDiscoveryRequestSubject() string {
	return "kkcluster.discovery.request"
}
