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
	// 用户自定义的集群请求处理器（用于透传非 actor.ask 的请求）
	userRequestHandler func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error)
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

	// 注册消息处理器，接收来自远程节点的 actor 消息（PublishRemote -> actor.message）
	rs.cluster.SetPublishHandler(rs.handleRemoteMessage)
	// 注册请求处理器，处理远程的 actor.ask 请求（RequestRemote）
	rs.cluster.SetRequestHandler(rs.handleClusterRequest)

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

	// 清除消息/请求处理器
	rs.cluster.SetPublishHandler(nil)
	rs.cluster.SetRequestHandler(nil)

	kklog.Infof("ClusterRemoteSystem stopped for node %s", rs.nodeID)
}

// Tell 向远程节点的 actor 发送消息（fire-and-forget）
// remotePID: 远程 actor 的 PID（Addr 是节点ID，ActorID 是 actor ID）
// message: 要发送的消息（如果是 []byte，直接传递；其他类型会被序列化为 JSON）
func (rs *ClusterRemoteSystem) Tell(remotePID RemotePID, message interface{}) error {
	if rs == nil || !rs.isStarted() {
		return nil
	}

	// 如果消息已经是 []byte，直接使用 TellBytes，避免 JSON 序列化
	if data, ok := message.([]byte); ok {
		return rs.TellBytes(remotePID, data)
	}

	// 其他类型序列化为 JSON
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

	// 如果消息已经是 []byte，直接使用，避免 JSON 序列化
	var msgData []byte
	if data, ok := message.([]byte); ok {
		msgData = data
	} else {
		// 其他类型序列化为 JSON
		var err error
		msgData, err = json.Marshal(message)
		if err != nil {
			kklog.Errorf("ClusterRemoteSystem marshal message failed: %v", err)
			return nil, false
		}
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

	// 通过 kkcluster 发送请求并等待响应（返回的是对端响应的 Data 字节）
	responseData, errCode := rs.cluster.RequestRemote(remotePID.Addr, packet, reqTimeout)
	if errCode != kkcluster.ClusterErrorCodeSuccess {
		return nil, false
	}
	// 直接返回字节数据，交由上层根据协议自行解码
	return responseData, true
}

// SetRequestHandler 设置用户自定义的集群请求处理器。
// ClusterRemoteSystem 会优先拦截并处理 FuncName == "actor.ask" 的请求，
// 其他 FuncName 将在本地处理完成后透传给该 handler（如果不为 nil）。
func (rs *ClusterRemoteSystem) SetRequestHandler(handler func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error)) {
	if rs == nil {
		return
	}
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.userRequestHandler = handler
}

// handleRemoteMessage 处理来自远程节点的发布消息（单向 actor.message）
func (rs *ClusterRemoteSystem) handleRemoteMessage(sourceNodeID string, packet *kkcluster.ClusterPacket) {
	if rs == nil || packet == nil {
		return
	}

	// 只处理单向 actor 消息
	if packet.FuncName != "actor.message" {
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

// handleClusterRequest 处理来自远程节点的请求消息（用于实现 actor 的 Ask 语义）
func (rs *ClusterRemoteSystem) handleClusterRequest(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error) {
	// 基本校验
	if rs == nil || req == nil || req.Packet == nil {
		return &kkcluster.ClusterResponse{
			RequestID: "",
			Code:      int32(kkcluster.ClusterErrorCodeInvalidRequest),
			Data:      nil,
		}, nil
	}

	// 如果不是 actor.ask，则透传给用户自定义的集群请求处理器（如有）
	if req.Packet.FuncName != "actor.ask" {
		rs.mu.RLock()
		handler := rs.userRequestHandler
		rs.mu.RUnlock()
		if handler != nil {
			return handler(req)
		}
		// 没有用户处理器，则返回无效请求错误
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeInvalidRequest),
			Data:      nil,
		}, nil
	}

	// 解析远程 actor 消息载体
	var actorMsg remoteEnvelope
	if err := json.Unmarshal(req.Packet.ArgBytes, &actorMsg); err != nil {
		kklog.Errorf("ClusterRemoteSystem handleClusterRequest: unmarshal actor message failed: %v", err)
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeInvalidData),
			Data:      nil,
		}, nil
	}
	if actorMsg.ActorID == "" {
		kklog.Warnf("ClusterRemoteSystem handleClusterRequest: empty actorID from %s", req.SourceNodeID)
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeInvalidRequest),
			Data:      nil,
		}, nil
	}

	// 获取本地 ActorSystem
	rs.mu.RLock()
	local := rs.local
	rs.mu.RUnlock()
	if local == nil {
		kklog.Warnf("ClusterRemoteSystem handleClusterRequest: no local actor system for request %s", req.RequestID)
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeFail),
			Data:      nil,
		}, nil
	}

	// 构造本地目标 actor 的 PID（本地 actor，nodeID 为空）
	targetPID := &PID{
		id:     actorMsg.ActorID,
		nodeID: "",
		system: local,
	}

	// 使用本地 Ask 语义向目标 actor 发送请求，并等待其通过 ctx.Send(ctx.Sender(), resp) 回复
	root := &RootContext{system: local}
	// 使用一个保守的超时时间（可根据需要调整或从请求中携带）
	timeout := 5 * time.Second
	if req.Packet.Timeout > 0 {
		timeout = time.Duration(req.Packet.Timeout) * time.Millisecond
	}
	resp, ok := root.Ask(targetPID, actorMsg.Data, timeout)
	if !ok {
		kklog.Errorf("ClusterRemoteSystem handleClusterRequest: actor %s no response for request %s", actorMsg.ActorID, req.RequestID)
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeTimeout),
			Data:      nil,
		}, nil
	}

	// 期望 actor 返回的是 []byte（业务可自行约定）
	var respBytes []byte
	switch v := resp.(type) {
	case []byte:
		respBytes = v
	case string:
		respBytes = []byte(v)
	default:
		// 其他类型统一走 JSON 序列化
		b, err := json.Marshal(v)
		if err != nil {
			kklog.Errorf("ClusterRemoteSystem handleClusterRequest: marshal actor response failed: %v", err)
			return &kkcluster.ClusterResponse{
				RequestID: req.RequestID,
				Code:      int32(kkcluster.ClusterErrorCodeMarshalFailed),
				Data:      nil,
			}, nil
		}
		respBytes = b
	}

	return &kkcluster.ClusterResponse{
		RequestID: req.RequestID,
		Code:      int32(kkcluster.ClusterErrorCodeSuccess),
		Data:      respBytes,
	}, nil
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
