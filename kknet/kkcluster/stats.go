package kkcluster

import (
	"sync/atomic"
)

// ClusterStatsSnapshot 集群统计信息快照
type ClusterStatsSnapshot struct {
	// 消息统计
	PublishSent      int64 // 发布消息数
	PublishReceived  int64 // 接收发布消息数
	RequestSent      int64 // 发送请求数
	RequestReceived  int64 // 接收请求数
	ResponseSent     int64 // 发送响应数
	ResponseReceived int64 // 接收响应数

	// 字节统计
	SentBytes     int64 // 发送字节数
	ReceivedBytes int64 // 接收字节数

	// 错误统计
	Errors int64 // 错误数

	// 连接统计
	Reconnects  int64 // 重连次数
	IsConnected bool  // 是否连接
}

// ClusterStats 集群统计信息
type ClusterStats struct {
	publishSent      int64
	publishReceived  int64
	requestSent      int64
	requestReceived  int64
	responseSent     int64
	responseReceived int64
	sentBytes        int64
	receivedBytes    int64
	errors           int64
	reconnects       int64
}

// AddPublishSent 记录发送的发布消息
func (s *ClusterStats) AddPublishSent(bytes int) {
	if bytes <= 0 {
		return
	}
	atomic.AddInt64(&s.publishSent, 1)
	atomic.AddInt64(&s.sentBytes, int64(bytes))
}

// AddPublishReceived 记录接收的发布消息
func (s *ClusterStats) AddPublishReceived(bytes int) {
	if bytes <= 0 {
		return
	}
	atomic.AddInt64(&s.publishReceived, 1)
	atomic.AddInt64(&s.receivedBytes, int64(bytes))
}

// AddRequestSent 记录发送的请求
func (s *ClusterStats) AddRequestSent(bytes int) {
	if bytes <= 0 {
		return
	}
	atomic.AddInt64(&s.requestSent, 1)
	atomic.AddInt64(&s.sentBytes, int64(bytes))
}

// AddRequestReceived 记录接收的请求
func (s *ClusterStats) AddRequestReceived(bytes int) {
	if bytes <= 0 {
		return
	}
	atomic.AddInt64(&s.requestReceived, 1)
	atomic.AddInt64(&s.receivedBytes, int64(bytes))
}

// AddResponseSent 记录发送的响应
func (s *ClusterStats) AddResponseSent(bytes int) {
	if bytes <= 0 {
		return
	}
	atomic.AddInt64(&s.responseSent, 1)
	atomic.AddInt64(&s.sentBytes, int64(bytes))
}

// AddResponseReceived 记录接收的响应
func (s *ClusterStats) AddResponseReceived(bytes int) {
	if bytes <= 0 {
		return
	}
	atomic.AddInt64(&s.responseReceived, 1)
	atomic.AddInt64(&s.receivedBytes, int64(bytes))
}

// AddError 记录错误
func (s *ClusterStats) AddError() {
	atomic.AddInt64(&s.errors, 1)
}

// AddReconnect 记录重连
func (s *ClusterStats) AddReconnect() {
	atomic.AddInt64(&s.reconnects, 1)
}

// Snapshot 获取统计快照
func (s *ClusterStats) Snapshot(isConnected bool) ClusterStatsSnapshot {
	return ClusterStatsSnapshot{
		PublishSent:      atomic.LoadInt64(&s.publishSent),
		PublishReceived:  atomic.LoadInt64(&s.publishReceived),
		RequestSent:      atomic.LoadInt64(&s.requestSent),
		RequestReceived:  atomic.LoadInt64(&s.requestReceived),
		ResponseSent:     atomic.LoadInt64(&s.responseSent),
		ResponseReceived: atomic.LoadInt64(&s.responseReceived),
		SentBytes:        atomic.LoadInt64(&s.sentBytes),
		ReceivedBytes:    atomic.LoadInt64(&s.receivedBytes),
		Errors:           atomic.LoadInt64(&s.errors),
		Reconnects:       atomic.LoadInt64(&s.reconnects),
		IsConnected:      isConnected,
	}
}

// Reset 重置统计信息
func (s *ClusterStats) Reset() {
	atomic.StoreInt64(&s.publishSent, 0)
	atomic.StoreInt64(&s.publishReceived, 0)
	atomic.StoreInt64(&s.requestSent, 0)
	atomic.StoreInt64(&s.requestReceived, 0)
	atomic.StoreInt64(&s.responseSent, 0)
	atomic.StoreInt64(&s.responseReceived, 0)
	atomic.StoreInt64(&s.sentBytes, 0)
	atomic.StoreInt64(&s.receivedBytes, 0)
	atomic.StoreInt64(&s.errors, 0)
	atomic.StoreInt64(&s.reconnects, 0)
}
