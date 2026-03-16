package dnats

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/kkmetrics"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kkevent"
	"github.com/vvisun/kkdg/utils/kklog"
)

const (
	subjectDiscovery        = "kkdiscovery.discovery"         // 服务发现主题
	subjectDiscoveryRequest = "kkdiscovery.discovery.request" // 服务发现请求主题
	subjectOffline          = "kkdiscovery.offline"           // 节点离线主题
)

// NatsDiscovery 基于NATS的服务发现实现
type NatsDiscovery struct {
	name     string
	nodeInfo *kkapp.NodeInfo

	conn       *nats.Conn
	sub        *nats.Subscription
	requestSub *nats.Subscription
	offlineSub *nats.Subscription // 订阅 subjectOffline，收到请求时响应以支持 Stop 提前结束

	memberMgr     *kkdiscovery.MemberMgr
	memberTimes   map[string]time.Time // 记录成员最后更新时间。key: nodeID, value: last update time
	memberTimesMu sync.RWMutex

	// 订阅管理（用于重连时重新订阅）
	subMu sync.Mutex

	// 启动标志（确保goroutine只启动一次）
	startedOnce sync.Once

	// 统计信息
	stats kkdiscovery.DiscoveryStats

	stopCh chan struct{}
	doneCh chan struct{}

	options      nats.Options
	discoveryOpt kkdiscovery.DiscoveryOption

	closing atomic.Bool // 正在关闭标志
	closed  atomic.Bool // 关闭标志

	msgCodec kkcodec.ICodec
}

var _ kkdiscovery.IDiscovery = (*NatsDiscovery)(nil)

// NewNatsDiscovery 创建新的NATS服务发现
func NewNatsDiscovery(
	name string,
	nodeInfo *kkapp.NodeInfo,
	natsOpts nats.Options,
	discoveryOpt kkdiscovery.DiscoveryOption,
) kkdiscovery.IDiscovery {
	d := &NatsDiscovery{
		name:         name,
		nodeInfo:     nodeInfo,
		memberMgr:    kkdiscovery.NewMemberMgr(),
		memberTimes:  make(map[string]time.Time), // key: nodeID, value: last update time
		stopCh:       make(chan struct{}),        // 停止通道
		doneCh:       make(chan struct{}),        // 完成通道
		options:      natsOpts,                   // NATS配置
		msgCodec:     discoveryOpt.MsgCodec,      // 消息编码器
		discoveryOpt: discoveryOpt,
	}

	// 订阅 discovery metrics 事件，通过 Stats 快照填充 MetricsEventData
	kkevent.GlobalBus.Subscribe(kkmetrics.EventDiscoveryMetrics, func(e *kkmetrics.MetricsEventData) {
		if e == nil {
			return
		}
		snap := d.Stats()
		e.Metrics = kkdiscovery.MetricsFromSnapshot(e.Namespace, snap)
	})

	return d
}

// Name 返回发现服务名称
func (d *NatsDiscovery) Name() string {
	return d.name
}

// IsRunning 是否已启动
func (d *NatsDiscovery) IsRunning() bool {
	return !d.closing.Load() && !d.closed.Load() && d.conn != nil && d.conn.IsConnected()
}

// GetMemberMgr 获取成员管理器
func (d *NatsDiscovery) GetMemberMgr() kkdiscovery.IMemberMgr {
	return d.memberMgr
}

// addMemberInfo 根据 MemberInfo 添加或更新成员
func (d *NatsDiscovery) addMemberInfo(info *kkdiscovery.MemberInfo) {
	if info == nil {
		return
	}

	_, isNew := d.memberMgr.AddMember(info)

	d.memberTimesMu.Lock()
	d.memberTimes[info.NodeID] = time.Now()
	d.memberTimesMu.Unlock()

	if isNew {
		d.stats.AddMember()
	}
}

// removeMember 移除成员
func (d *NatsDiscovery) removeMember(nodeID string) {
	if nodeID == "" {
		return
	}

	existed := d.memberMgr.RemoveMember(nodeID)
	if !existed {
		return
	}

	d.memberTimesMu.Lock()
	delete(d.memberTimes, nodeID)
	d.memberTimesMu.Unlock()

	d.stats.RemoveMember()
}

// Stats 获取统计信息快照
func (d *NatsDiscovery) Stats() kkdiscovery.DiscoveryStatsSnapshot {
	memberCount := d.memberMgr.MemberCount()
	isConnected := d.conn != nil && d.conn.IsConnected()
	return d.stats.Snapshot(memberCount, isConnected)
}

// Stop 停止服务发现
func (d *NatsDiscovery) Stop() error {
	d.closing.Store(true)
	kklog.Infof("NatsDiscovery(%s) shutdown", d.nodeInfo.GetNodeId())

	d.publishSelf() // 将离线通知出去

	//阻塞发一个请求，返回时表示离线通知已发出
	if d.conn != nil {
		ctx, cancel := context.WithTimeout(context.Background(), d.discoveryOpt.OfflineTimeout)
		defer cancel()
		_, err := d.conn.RequestWithContext(ctx, subjectOffline, []byte(""))
		if err != nil {
			kklog.Errorf("NatsDiscovery(%s) send offline notification failed: %v", d.nodeInfo.GetNodeId(), err)
		}
	}

	kkevent.GlobalBus.UnsubscribeAll(kkmetrics.EventDiscoveryMetrics)

	select {
	case <-d.stopCh:
		return nil
	default:
		d.closed.Store(true) // 先设置 closed，避免 goroutine 中请求时打 error log
		close(d.stopCh)
	}

	if d.sub != nil {
		_ = d.sub.Unsubscribe()
	}
	if d.requestSub != nil {
		_ = d.requestSub.Unsubscribe()
	}
	if d.offlineSub != nil {
		_ = d.offlineSub.Unsubscribe()
	}
	if d.conn != nil {
		d.conn.Close()
	}

	close(d.doneCh)
	return nil
}

// Start 启动服务发现（需要在外部调用）
func (d *NatsDiscovery) Start() error {
	kklog.Infof("NatsDiscovery(%s) startup", d.nodeInfo.GetNodeId())
	return d.connectAndSubscribe()
}

// connectAndSubscribe 连接NATS并订阅主题
func (d *NatsDiscovery) connectAndSubscribe() error {
	// 配置NATS连接选项，启用自动重连
	opts := &d.options

	// 设置重连处理器
	opts.ReconnectedCB = func(nc *nats.Conn) {
		if d.closing.Load() {
			return
		}
		kklog.Infof("NatsDiscovery(%s) reconnected to %s", d.nodeInfo.GetNodeId(), nc.ConnectedUrl())
		d.stats.AddReconnect()
		// 重连后重新订阅
		if err := d.resubscribe(); err != nil {
			kklog.Errorf("NatsDiscovery(%s) resubscribe failed: %v", d.nodeInfo.GetNodeId(), err)
			d.stats.AddError()
		}
		// 重连后立即发布自己的信息
		if err := d.publishSelf(); err != nil {
			kklog.Warnf("NatsDiscovery(%s) publish self after reconnect failed: %v", d.nodeInfo.GetNodeId(), err)
			d.stats.AddError()
		}
		// 重连后立即请求所有成员
		go d.requestAllMembers()
	}

	// 设置断开连接处理器
	opts.DisconnectedErrCB = func(nc *nats.Conn, err error) {
		if err != nil {
			kklog.Warnf("NatsDiscovery(%s) disconnected: %s, %v", d.nodeInfo.GetNodeId(), nc.Opts.Name, err)
		} else {
			kklog.Warnf("NatsDiscovery(%s) disconnected: %s", d.nodeInfo.GetNodeId(), nc.Opts.Name)
		}
	}

	// 设置关闭处理器
	opts.ClosedCB = func(nc *nats.Conn) {
		kklog.Infof("NatsDiscovery(%s) connection closed: %s", d.nodeInfo.GetNodeId(), nc.Opts.Name)
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
		kklog.Warnf("NatsDiscovery(%s) publish self failed: %v", d.nodeInfo.GetNodeId(), err)
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
	if d.offlineSub != nil {
		_ = d.offlineSub.Unsubscribe()
		d.offlineSub = nil
	}

	// 订阅服务发现主题
	sub, err := d.conn.Subscribe(subjectDiscovery, d.handleDiscoveryMessage)
	if err != nil {
		return err
	}
	d.sub = sub

	// 订阅服务发现请求主题（用于响应其他节点的请求）
	requestSub, err := d.conn.Subscribe(subjectDiscoveryRequest, d.handleDiscoveryRequest)
	if err != nil {
		sub.Unsubscribe()
		return err
	}
	d.requestSub = requestSub

	// 订阅离线主题，收到请求时响应，使 Stop 中的 Request 可提前返回
	offlineSub, err := d.conn.Subscribe(subjectOffline, d.handleOfflineRequest)
	if err != nil {
		sub.Unsubscribe()
		requestSub.Unsubscribe()
		return err
	}
	d.offlineSub = offlineSub

	return nil
}

// handleOfflineRequest 处理 subjectOffline 请求，响应后 Stop 中的 Request 可提前返回
func (d *NatsDiscovery) handleOfflineRequest(msg *nats.Msg) {
	_ = msg.Respond([]byte("ok"))
}

// handleDiscoveryMessage 处理服务发现消息
func (d *NatsDiscovery) handleDiscoveryMessage(msg *nats.Msg) {
	// 记录心跳接收统计
	d.stats.AddHeartbeatReceived()

	var memberInfo kkdiscovery.MemberInfo
	if err := d.msgCodec.Unmarshal(msg.Data, &memberInfo); err != nil {
		kklog.Errorf("NatsDiscovery(%s) unmarshal member info failed: %v", d.nodeInfo.GetNodeId(), err)
		d.stats.AddError()
		return
	}

	// 忽略自己
	if memberInfo.NodeID == d.nodeInfo.GetNodeId() {
		return
	}

	// 利用 MemberMgr 统一管理成员信息
	d.addMemberInfo(&memberInfo)
}

// publishSelf 发布自己的信息
func (d *NatsDiscovery) publishSelf() error {
	if d.closed.Load() {
		return nil // 已关闭，静默返回
	}
	if d.conn == nil {
		return nil // 未连接，静默返回
	}

	evt := &kkdiscovery.DiscoveryStatsEvent{
		OnlineCount: 0,
		Status:      kkdiscovery.NodeStatusOnline,
	}
	kkevent.GlobalBus.Publish(kkdiscovery.EventDiscoveryStats, evt)

	if d.closing.Load() {
		evt.Status = kkdiscovery.NodeStatusOffline // 正在关闭，设置为离线
	}

	memberInfo := kkdiscovery.MemberInfo{
		NodeID:     d.nodeInfo.GetNodeId(),
		NodeType:   d.nodeInfo.GetNodeType(),
		Address:    d.nodeInfo.GetAddress(),
		RpcAddress: d.nodeInfo.GetRpcAddress(),
		Weight:     evt.OnlineCount,
		Status:     evt.Status,
	}

	data, err := d.msgCodec.Marshal(&memberInfo)
	if err != nil {
		d.stats.AddError()
		kklog.Errorf("NatsDiscovery(%s) marshal self failed: nodeType=%s addr=%s err=%v",
			d.nodeInfo.GetNodeId(), d.nodeInfo.GetNodeType(), d.nodeInfo.GetAddress(), err)
		return err
	}

	if err := d.conn.Publish(subjectDiscovery, data); err != nil {
		d.stats.AddError()
		kklog.Errorf("NatsDiscovery(%s) publish self failed: subject=%s bytes=%d err=%v",
			d.nodeInfo.GetNodeId(), subjectDiscovery, len(data), err)
		return err
	}

	// 记录心跳发送统计
	d.stats.AddHeartbeatSent()
	return nil
}

// heartbeatLoop 心跳循环
func (d *NatsDiscovery) heartbeatLoop() {
	ticker := time.NewTicker(defaultPublishSelfInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			if d.closed.Load() {
				return
			}
			if err := d.publishSelf(); err != nil {
				d.stats.AddError()
				kklog.Errorf("NatsDiscovery(%s) heartbeat failed: %v", d.nodeInfo.GetNodeId(), err)
			}
		}
	}
}

// requestAllMembers 请求所有成员
func (d *NatsDiscovery) requestAllMembers() {
	// 延迟一下，等待连接稳定
	time.Sleep(1 * time.Second)
	if d.closing.Load() {
		return
	}

	// 发送请求消息
	reqMsg := kkdiscovery.DiscoveryRequest{
		RequesterID: d.nodeInfo.GetNodeId(),
	}

	data, err := d.msgCodec.Marshal(&reqMsg)
	if err != nil {
		d.stats.AddError()
		kklog.Errorf("NatsDiscovery(%s) marshal request failed: requesterID=%s err=%v",
			d.nodeInfo.GetNodeId(), reqMsg.RequesterID, err)
		return
	}

	if d.conn != nil && !d.closed.Load() {
		if err := d.conn.Publish(subjectDiscoveryRequest, data); err != nil {
			d.stats.AddError()
			kklog.Errorf("NatsDiscovery(%s) publish request failed: subject=%s requesterID=%s bytes=%d err=%v",
				d.nodeInfo.GetNodeId(), subjectDiscoveryRequest, reqMsg.RequesterID, len(data), err)
		}
	}
}

// handleDiscoveryRequest 处理服务发现请求
func (d *NatsDiscovery) handleDiscoveryRequest(msg *nats.Msg) {
	var req kkdiscovery.DiscoveryRequest
	if err := d.msgCodec.Unmarshal(msg.Data, &req); err != nil {
		kklog.Errorf("NatsDiscovery(%s) unmarshal request failed: %v", d.nodeInfo.GetNodeId(), err)
		d.stats.AddError()
		return
	}

	// 忽略自己的请求
	if req.RequesterID == d.nodeInfo.GetNodeId() {
		return
	}

	// 响应自己的信息
	if err := d.publishSelf(); err != nil {
		kklog.Errorf("NatsDiscovery(%s) respond to request failed: %v", d.nodeInfo.GetNodeId(), err)
		// publishSelf内部已经记录了错误统计
	}
}

// checkMemberTimeout 检查成员超时
func (d *NatsDiscovery) checkMemberTimeout() {
	ticker := time.NewTicker(defaultCheckMemberInterval)
	defer ticker.Stop()

	timeout := defaultMemberTimeout // 成员超时时间

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			now := time.Now()
			var toRemove []string

			d.memberTimesMu.RLock()
			for nodeID, lastTime := range d.memberTimes {
				if now.Sub(lastTime) > timeout {
					toRemove = append(toRemove, nodeID)
				}
			}
			d.memberTimesMu.RUnlock()

			for _, nodeID := range toRemove {
				kklog.Warnf("NatsDiscovery(%s) member %s timeout, removing", d.nodeInfo.GetNodeId(), nodeID)
				d.removeMember(nodeID)
			}
		}
	}
}
