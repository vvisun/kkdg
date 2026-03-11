package dnats

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/utils/kkcodec"
	"github.com/vvisun/kkdg/utils/kklog"
)

// NatsDiscovery 基于NATS的服务发现实现
type NatsDiscovery struct {
	name     string
	nodeID   string
	nodeType string
	address  string
	settings map[string]string

	conn       *nats.Conn
	sub        *nats.Subscription
	requestSub *nats.Subscription

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

	options nats.Options

	closing atomic.Bool // 正在关闭标志
	closed  atomic.Bool // 关闭标志

	msgCodec     kkcodec.ICodec
	infoGetterFn func() (int, int) // return (onlineCount, status)。在线数量，状态
}

var _ kkdiscovery.IDiscovery = (*NatsDiscovery)(nil)

// NewNatsDiscovery 创建新的NATS服务发现
func NewNatsDiscovery(name string, nodeInfo *kkapp.NodeInfo, settings map[string]string, opts nats.Options) kkdiscovery.IDiscovery {
	if settings == nil {
		settings = make(map[string]string)
	}
	return &NatsDiscovery{
		name:        name,
		nodeID:      nodeInfo.GetNodeId(),
		nodeType:    nodeInfo.GetNodeType(),
		address:     nodeInfo.GetAddress(),
		settings:    settings,
		memberMgr:   kkdiscovery.NewMemberMgr(),
		memberTimes: make(map[string]time.Time), // key: nodeID, value: last update time
		stopCh:      make(chan struct{}),
		doneCh:      make(chan struct{}),
		options:     opts,
		msgCodec:    kkcodec.GetCodec(kkcodec.CodecTypeMsgpack),
	}
}

// Name 返回发现服务名称
func (d *NatsDiscovery) Name() string {
	return d.name
}

// SetMsgCodec 设置消息编码器
func (d *NatsDiscovery) SetMsgCodec(codec kkcodec.ICodec) {
	if codec == nil {
		kklog.Errorf("[kkdiscovery] SetMsgCodec codec is nil, use default codec: %s", "msgpack")
		codec = kkcodec.GetCodec(kkcodec.CodecTypeMsgpack)
	}
	d.msgCodec = codec
}

// SetInfoGetter 设置信息获取函数
func (d *NatsDiscovery) SetInfoGetter(fn func() (int, int)) {
	d.infoGetterFn = fn
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
	_, existed := d.memberMgr.GetMember(info.NodeID)
	d.memberMgr.AddMember(info)

	d.memberTimesMu.Lock()
	d.memberTimes[info.NodeID] = time.Now()
	d.memberTimesMu.Unlock()

	if !existed {
		d.stats.AddMember()
	}
}

// removeMember 移除成员
func (d *NatsDiscovery) removeMember(nodeID string) {
	if nodeID == "" {
		return
	}
	_, existed := d.memberMgr.GetMember(nodeID)
	if !existed {
		return
	}

	d.memberMgr.RemoveMember(nodeID)

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
	kklog.Infof("NatsDiscovery(%s) shutdown", d.nodeID)

	d.publishSelf()                    // 将离线通知出去
	time.Sleep(500 * time.Millisecond) // 等待500毫秒，让离线通知出去

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
	if d.conn != nil {
		d.conn.Close()
	}

	close(d.doneCh)
	return nil
}

// Start 启动服务发现（需要在外部调用）
func (d *NatsDiscovery) Start() error {
	kklog.Infof("NatsDiscovery(%s) startup", d.nodeID)
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
		kklog.Infof("NatsDiscovery(%s) reconnected to %s", d.nodeID, nc.ConnectedUrl())
		d.stats.AddReconnect()
		// 重连后重新订阅
		if err := d.resubscribe(); err != nil {
			kklog.Errorf("NatsDiscovery(%s) resubscribe failed: %v", d.nodeID, err)
			d.stats.AddError()
		}
		// 重连后立即发布自己的信息
		if err := d.publishSelf(); err != nil {
			kklog.Warnf("NatsDiscovery(%s) publish self after reconnect failed: %v", d.nodeID, err)
			d.stats.AddError()
		}
	}

	// 设置断开连接处理器
	opts.DisconnectedErrCB = func(nc *nats.Conn, err error) {
		if err != nil {
			kklog.Warnf("NatsDiscovery(%s) disconnected: %s, %v", d.nodeID, nc.Opts.Name, err)
		} else {
			kklog.Warnf("NatsDiscovery(%s) disconnected: %s", d.nodeID, nc.Opts.Name)
		}
	}

	// 设置关闭处理器
	opts.ClosedCB = func(nc *nats.Conn) {
		kklog.Infof("NatsDiscovery(%s) connection closed: %s", d.nodeID, nc.Opts.Name)
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
		kklog.Warnf("NatsDiscovery(%s) publish self failed: %v", d.nodeID, err)
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
	if err := d.msgCodec.Unmarshal(msg.Data, &memberInfo); err != nil {
		kklog.Errorf("NatsDiscovery(%s) unmarshal member info failed: %v", d.nodeID, err)
		d.stats.AddError()
		return
	}

	// 忽略自己
	if memberInfo.NodeID == d.nodeID {
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
	weight, status := 0, 0
	if d.infoGetterFn != nil {
		weight, status = d.infoGetterFn()
	}
	if d.closing.Load() {
		status = kkdiscovery.NodeStatusOffline // 正在关闭，设置为离线
	}

	memberInfo := kkdiscovery.MemberInfo{
		NodeID:   d.nodeID,
		NodeType: d.nodeType,
		Address:  d.address,
		Weight:   weight,
		Status:   status,
		Settings: d.settings,
	}

	data, err := d.msgCodec.Marshal(&memberInfo)
	if err != nil {
		d.stats.AddError()
		kklog.Errorf("NatsDiscovery(%s) marshal self failed: nodeType=%s addr=%s err=%v", d.nodeID, d.nodeType, d.address, err)
		return err
	}

	subject := d.getDiscoverySubject()
	if err := d.conn.Publish(subject, data); err != nil {
		d.stats.AddError()
		kklog.Errorf("NatsDiscovery(%s) publish self failed: subject=%s bytes=%d err=%v", d.nodeID, subject, len(data), err)
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
				kklog.Errorf("NatsDiscovery(%s) heartbeat failed: %v", d.nodeID, err)
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
		RequesterID: d.nodeID,
	}

	data, err := d.msgCodec.Marshal(&reqMsg)
	if err != nil {
		d.stats.AddError()
		kklog.Errorf("NatsDiscovery(%s) marshal discovery request failed: requesterID=%s err=%v", d.nodeID, reqMsg.RequesterID, err)
		return
	}

	subject := d.getDiscoveryRequestSubject()
	if d.conn != nil && !d.closed.Load() {
		if err := d.conn.Publish(subject, data); err != nil {
			d.stats.AddError()
			kklog.Errorf("NatsDiscovery(%s) publish discovery request failed: subject=%s requesterID=%s bytes=%d err=%v", d.nodeID, subject, reqMsg.RequesterID, len(data), err)
		}
	}
}

// handleDiscoveryRequest 处理服务发现请求
func (d *NatsDiscovery) handleDiscoveryRequest(msg *nats.Msg) {
	var req kkdiscovery.DiscoveryRequest
	if err := d.msgCodec.Unmarshal(msg.Data, &req); err != nil {
		kklog.Errorf("NatsDiscovery(%s) unmarshal request failed: %v", d.nodeID, err)
		d.stats.AddError()
		return
	}

	// 忽略自己的请求
	if req.RequesterID == d.nodeID {
		return
	}

	// 响应自己的信息
	if err := d.publishSelf(); err != nil {
		kklog.Errorf("NatsDiscovery(%s) respond to request failed: %v", d.nodeID, err)
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
				kklog.Warnf("NatsDiscovery(%s) member %s timeout, removing", d.nodeID, nodeID)
				d.removeMember(nodeID)
			}
		}
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
