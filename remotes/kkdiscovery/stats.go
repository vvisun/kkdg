package kkdiscovery

import "sync/atomic"

// DiscoveryStatsSnapshot 服务发现统计信息快照
type DiscoveryStatsSnapshot struct {
	// 成员统计
	MemberCount    int64 // 当前成员数
	MembersAdded   int64 // 成员添加次数
	MembersRemoved int64 // 成员移除次数

	// 心跳统计
	HeartbeatsSent     int64 // 心跳发送次数
	HeartbeatsReceived int64 // 心跳接收次数

	// 错误统计
	Errors int64 // 错误数

	// 连接统计
	Reconnects  int64 // 重连次数
	IsConnected bool  // 是否连接
}

// DiscoveryStats 服务发现统计信息
type DiscoveryStats struct {
	memberCount        int64
	membersAdded       int64
	membersRemoved     int64
	heartbeatsSent     int64
	heartbeatsReceived int64
	errors             int64
	reconnects         int64
}

// AddMember 记录成员添加
func (s *DiscoveryStats) AddMember() {
	atomic.AddInt64(&s.memberCount, 1)
	atomic.AddInt64(&s.membersAdded, 1)
}

// RemoveMember 记录成员移除
func (s *DiscoveryStats) RemoveMember() {
	atomic.AddInt64(&s.memberCount, -1)
	atomic.AddInt64(&s.membersRemoved, 1)
}

// AddHeartbeatSent 记录心跳发送
func (s *DiscoveryStats) AddHeartbeatSent() {
	atomic.AddInt64(&s.heartbeatsSent, 1)
}

// AddHeartbeatReceived 记录心跳接收
func (s *DiscoveryStats) AddHeartbeatReceived() {
	atomic.AddInt64(&s.heartbeatsReceived, 1)
}

// AddError 记录错误
func (s *DiscoveryStats) AddError() {
	atomic.AddInt64(&s.errors, 1)
}

// AddReconnect 记录重连
func (s *DiscoveryStats) AddReconnect() {
	atomic.AddInt64(&s.reconnects, 1)
}

// Snapshot 获取统计快照
func (s *DiscoveryStats) Snapshot(memberCount int, isConnected bool) DiscoveryStatsSnapshot {
	return DiscoveryStatsSnapshot{
		MemberCount:        int64(memberCount),
		MembersAdded:       atomic.LoadInt64(&s.membersAdded),
		MembersRemoved:     atomic.LoadInt64(&s.membersRemoved),
		HeartbeatsSent:     atomic.LoadInt64(&s.heartbeatsSent),
		HeartbeatsReceived: atomic.LoadInt64(&s.heartbeatsReceived),
		Errors:             atomic.LoadInt64(&s.errors),
		Reconnects:         atomic.LoadInt64(&s.reconnects),
		IsConnected:        isConnected,
	}
}

// Reset 重置统计信息
func (s *DiscoveryStats) Reset() {
	atomic.StoreInt64(&s.memberCount, 0)
	atomic.StoreInt64(&s.membersAdded, 0)
	atomic.StoreInt64(&s.membersRemoved, 0)
	atomic.StoreInt64(&s.heartbeatsSent, 0)
	atomic.StoreInt64(&s.heartbeatsReceived, 0)
	atomic.StoreInt64(&s.errors, 0)
	atomic.StoreInt64(&s.reconnects, 0)
}

//----------------------------------------------------------

// MetricsFromSnapshot 将统计快照转换为可用于 metrics 导出的键值对。
//
// - namespace 用于区分不同实例/模块，为空则不加前缀。
// - 返回的 key 采用 "<namespace>.<name>" 或仅 "<name>" 形式，值统一为 float64 方便接入 Prometheus、StatsD 等。
//
// 约定的度量名称（未加 namespace）:
//   - discovery_member_count
//   - discovery_members_added_total
//   - discovery_members_removed_total
//   - discovery_heartbeats_sent_total
//   - discovery_heartbeats_received_total
//   - discovery_errors_total
//   - discovery_reconnects_total
//   - discovery_is_connected (0 或 1)
func MetricsFromSnapshot(namespace string, snap DiscoveryStatsSnapshot) map[string]float64 {
	prefix := ""
	if namespace != "" {
		prefix = namespace + "."
	}
	m := make(map[string]float64, 8)
	m[prefix+"discovery_member_count"] = float64(snap.MemberCount)
	m[prefix+"discovery_members_added_total"] = float64(snap.MembersAdded)
	m[prefix+"discovery_members_removed_total"] = float64(snap.MembersRemoved)
	m[prefix+"discovery_heartbeats_sent_total"] = float64(snap.HeartbeatsSent)
	m[prefix+"discovery_heartbeats_received_total"] = float64(snap.HeartbeatsReceived)
	m[prefix+"discovery_errors_total"] = float64(snap.Errors)
	m[prefix+"discovery_reconnects_total"] = float64(snap.Reconnects)
	if snap.IsConnected {
		m[prefix+"discovery_is_connected"] = 1
	} else {
		m[prefix+"discovery_is_connected"] = 0
	}
	return m
}

