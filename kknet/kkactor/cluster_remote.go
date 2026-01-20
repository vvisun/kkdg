package kkactor

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/vvisun/kkdg/kknet/kkcluster"
	"github.com/vvisun/kkdg/utils/kklog"
)

// ClusterRemoteSystem 基于 kkcluster (NATS) 的多节点 actor 远程通信系统
type ClusterRemoteSystem struct {
	local     *ActorSystem
	cluster   kkcluster.ICluster
	discovery kkcluster.IDiscovery
	nodeID    string
	mu        sync.RWMutex
	started   bool
}

// NewClusterRemoteSystem 创建一个基于 kkcluster 的远程 actor 系统
// nodeID: 当前节点的 ID（用于标识消息来源）
// cluster: kkcluster 实例（必须已初始化）
// discovery: 服务发现实例（用于节点发现）
func NewClusterRemoteSystem(nodeID string, cluster kkcluster.ICluster, discovery kkcluster.IDiscovery) *ClusterRemoteSystem {
	if nodeID == "" || cluster == nil || discovery == nil {
		return nil
	}

	rs := &ClusterRemoteSystem{
		local:     nil, // 稍后通过 Start 设置
		cluster:   cluster,
		discovery: discovery,
		nodeID:    nodeID,
	}

	return rs
}

// Start 启动远程系统，绑定到本地 ActorSystem
func (rs *ClusterRemoteSystem) Start(local *ActorSystem) error {
	if rs == nil || local == nil {
		return nil
	}

	rs.mu.Lock()
	if rs.started {
		rs.mu.Unlock()
		return nil
	}
	rs.local = local
	rs.started = true
	rs.mu.Unlock()

	// 注册消息处理器，接收来自远程节点的 actor 消息
	rs.cluster.SetPublishHandler(rs.handleRemoteMessage)

	kklog.Infof("ClusterRemoteSystem started for node %s", rs.nodeID)
	return nil
}

// Stop 停止远程系统
func (rs *ClusterRemoteSystem) Stop() {
	if rs == nil {
		return
	}

	rs.mu.Lock()
	if !rs.started {
		rs.mu.Unlock()
		return
	}
	rs.started = false
	rs.mu.Unlock()

	// 清除消息处理器
	rs.cluster.SetPublishHandler(nil)

	kklog.Infof("ClusterRemoteSystem stopped for node %s", rs.nodeID)
}

// Tell 向远程节点的 actor 发送消息（fire-and-forget）
// remotePID: 远程 actor 的 PID（Addr 是节点ID，ActorID 是 actor ID）
// message: 要发送的消息（会被序列化为 JSON）
func (rs *ClusterRemoteSystem) Tell(remotePID RemotePID, message interface{}) error {
	if rs == nil || !rs.isStarted() {
		return nil
	}

	// 序列化消息
	msgData, err := json.Marshal(message)
	if err != nil {
		kklog.Errorf("ClusterRemoteSystem marshal message failed: %v", err)
		return err
	}

	// 创建 actor 消息包
	actorMsg := remoteEnvelope{
		ActorID: remotePID.ActorID,
		Data:    msgData,
	}

	// 序列化 actor 消息包
	actorMsgData, err := json.Marshal(&actorMsg)
	if err != nil {
		kklog.Errorf("ClusterRemoteSystem marshal actor message failed: %v", err)
		return err
	}

	// 创建集群消息包
	packet := &kkcluster.ClusterPacket{
		SourcePath: rs.nodeID,
		TargetPath: remotePID.Addr,  // 目标节点ID
		FuncName:   "actor.message", // 标识这是 actor 消息
		ArgBytes:   actorMsgData,
	}

	// 通过 kkcluster 发送到远程节点
	return rs.cluster.PublishRemote(remotePID.Addr, packet)
}

// TellBytes 向远程节点的 actor 发送二进制消息（fire-and-forget）
// remotePID: 远程 actor 的 PID
// data: 要发送的二进制数据
func (rs *ClusterRemoteSystem) TellBytes(remotePID RemotePID, data []byte) error {
	if rs == nil || !rs.isStarted() {
		return nil
	}

	// 创建 actor 消息包
	actorMsg := remoteEnvelope{
		ActorID: remotePID.ActorID,
		Data:    data,
	}

	// 序列化 actor 消息包
	actorMsgData, err := json.Marshal(&actorMsg)
	if err != nil {
		kklog.Errorf("ClusterRemoteSystem marshal actor message failed: %v", err)
		return err
	}

	// 创建集群消息包
	packet := &kkcluster.ClusterPacket{
		SourcePath: rs.nodeID,
		TargetPath: remotePID.Addr,  // 目标节点ID
		FuncName:   "actor.message", // 标识这是 actor 消息
		ArgBytes:   actorMsgData,
	}

	// 通过 kkcluster 发送到远程节点
	return rs.cluster.PublishRemote(remotePID.Addr, packet)
}

// Ask 向远程节点的 actor 发送消息并等待响应（同步请求-响应）
// remotePID: 远程 actor 的 PID
// message: 要发送的消息
// timeout: 超时时间
// 返回: 响应消息和是否成功
func (rs *ClusterRemoteSystem) Ask(remotePID RemotePID, message interface{}, timeout ...time.Duration) (reply interface{}, ok bool) {
	if rs == nil || !rs.isStarted() {
		return nil, false
	}

	// 序列化消息
	msgData, err := json.Marshal(message)
	if err != nil {
		kklog.Errorf("ClusterRemoteSystem marshal message failed: %v", err)
		return nil, false
	}

	// 创建 actor 消息包
	actorMsg := remoteEnvelope{
		ActorID: remotePID.ActorID,
		Data:    msgData,
	}

	// 序列化 actor 消息包
	actorMsgData, err := json.Marshal(&actorMsg)
	if err != nil {
		kklog.Errorf("ClusterRemoteSystem marshal actor message failed: %v", err)
		return nil, false
	}

	// 创建集群消息包
	packet := &kkcluster.ClusterPacket{
		SourcePath: rs.nodeID,
		TargetPath: remotePID.Addr, // 目标节点ID
		FuncName:   "actor.ask",    // 标识这是 actor ask 消息
		ArgBytes:   actorMsgData,
	}

	// 设置超时
	reqTimeout := 5 * time.Second
	if len(timeout) > 0 && timeout[0] > 0 {
		reqTimeout = timeout[0]
	}

	// 通过 kkcluster 发送请求并等待响应
	responseData, errCode := rs.cluster.RequestRemote(remotePID.Addr, packet, reqTimeout)
	if errCode != kkcluster.ClusterErrorCodeSuccess {
		return nil, false
	}

	// 反序列化响应
	var replyMsg interface{}
	if err := json.Unmarshal(responseData, &replyMsg); err != nil {
		kklog.Errorf("ClusterRemoteSystem unmarshal reply failed: %v", err)
		return nil, false
	}

	return replyMsg, true
}

// handleRemoteMessage 处理来自远程节点的消息
func (rs *ClusterRemoteSystem) handleRemoteMessage(sourceNodeID string, packet *kkcluster.ClusterPacket) {
	if rs == nil || packet == nil {
		return
	}

	// 只处理 actor 消息
	if packet.FuncName != "actor.message" && packet.FuncName != "actor.ask" {
		return
	}

	// 反序列化 actor 消息包
	var actorMsg remoteEnvelope
	if err := json.Unmarshal(packet.ArgBytes, &actorMsg); err != nil {
		kklog.Errorf("ClusterRemoteSystem unmarshal actor message failed: %v", err)
		return
	}

	if actorMsg.ActorID == "" {
		kklog.Warnf("ClusterRemoteSystem got message without actorId from %s", sourceNodeID)
		return
	}

	// 投递到本地 ActorSystem
	rs.mu.RLock()
	local := rs.local
	rs.mu.RUnlock()

	if local != nil {
		// 将 Data 作为消息体（[]byte）投递到对应本地 actor
		local.sendByID(actorMsg.ActorID, actorMsg.Data, nil)
	}
}

// isStarted 检查系统是否已启动
func (rs *ClusterRemoteSystem) isStarted() bool {
	if rs == nil {
		return false
	}
	rs.mu.RLock()
	defer rs.mu.RUnlock()
	return rs.started
}

// GetNodeID 获取当前节点ID
func (rs *ClusterRemoteSystem) GetNodeID() string {
	if rs == nil {
		return ""
	}
	return rs.nodeID
}

// GetDiscovery 获取服务发现实例
func (rs *ClusterRemoteSystem) GetDiscovery() kkcluster.IDiscovery {
	if rs == nil {
		return nil
	}
	return rs.discovery
}
