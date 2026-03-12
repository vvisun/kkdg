package kkrpc

import "sync/atomic"

// RpcStatsSnapshot RPC 统计信息快照。
//
// 这些字段尽量保持通用，方便在不同调用场景下做统一观测：
//   - 请求相关：总请求数 / 成功数 / 失败数 / 超时数
//   - 单向调用：总次数 / 失败次数
//   - 其它错误：编码/解码等内部错误次数
//   - Pending：当前挂起请求数与允许的最大挂起数（通常来自 pendingMap）
type RpcStatsSnapshot struct {
	// 请求统计
	RequestsTotal    int64 // 请求总数（含成功/失败/超时）
	RequestsSuccess  int64 // 成功完成的请求数
	RequestsError    int64 // 业务或协议错误数（ErrRpc 非空）
	RequestsTimeout  int64 // 超时请求数
	RequestsCanceled int64 // 上下文取消的请求数（context.Canceled/DeadlineExceeded）

	// 单向调用统计
	OnewayTotal int64 // 单向调用总数
	OnewayError int64 // 单向调用失败数（编码/发送失败）

	// 内部错误统计（编码/解码/内部状态等）
	InternalErrors int64 // 内部错误总数

	// 挂起请求统计
	PendingCurrent int64 // 当前挂起请求数（pendingMap.curPendingCount）
	PendingMax     int64 // 最大允许挂起请求数（pendingMap.maxPendingCount）
}

// RpcStats RPC 统计信息。通过原子操作累加计数，在热点路径上开销较小。
type RpcStats struct {
	requestsTotal    int64
	requestsSuccess  int64
	requestsError    int64
	requestsTimeout  int64
	requestsCanceled int64

	onewayTotal int64
	onewayError int64

	internalErrors int64
}

// AddRequestStart 记录一次请求发起（Invoke/InvokeAsync）。
func (s *RpcStats) AddRequestStart() {
	atomic.AddInt64(&s.requestsTotal, 1)
}

// AddRequestSuccess 记录一次请求成功完成（无 ErrRpc 且解码成功）。
func (s *RpcStats) AddRequestSuccess() {
	atomic.AddInt64(&s.requestsSuccess, 1)
}

// AddRequestError 记录一次业务或协议错误（ErrRpc 非空，或解码失败等）。
func (s *RpcStats) AddRequestError() {
	atomic.AddInt64(&s.requestsError, 1)
}

// AddRequestTimeout 记录一次超时（ErrRpcTimeout）。
func (s *RpcStats) AddRequestTimeout() {
	atomic.AddInt64(&s.requestsTimeout, 1)
}

// AddRequestCanceled 记录一次上下文取消（context.Canceled/DeadlineExceeded）。
func (s *RpcStats) AddRequestCanceled() {
	atomic.AddInt64(&s.requestsCanceled, 1)
}

// AddOnewayStart 记录一次单向调用发起。
func (s *RpcStats) AddOnewayStart() {
	atomic.AddInt64(&s.onewayTotal, 1)
}

// AddOnewayError 记录一次单向调用失败（编码/发送失败）。
func (s *RpcStats) AddOnewayError() {
	atomic.AddInt64(&s.onewayError, 1)
}

// AddInternalError 记录一次内部错误（编码/解码/状态异常等）。
func (s *RpcStats) AddInternalError() {
	atomic.AddInt64(&s.internalErrors, 1)
}

// Snapshot 构造统计快照。
//
// pendingCurrent/pendingMax 一般来自 pendingMap（curPendingCount / maxPendingCount），由调用方传入。
func (s *RpcStats) Snapshot(pendingCurrent, pendingMax int64) RpcStatsSnapshot {
	return RpcStatsSnapshot{
		RequestsTotal:    atomic.LoadInt64(&s.requestsTotal),
		RequestsSuccess:  atomic.LoadInt64(&s.requestsSuccess),
		RequestsError:    atomic.LoadInt64(&s.requestsError),
		RequestsTimeout:  atomic.LoadInt64(&s.requestsTimeout),
		RequestsCanceled: atomic.LoadInt64(&s.requestsCanceled),

		OnewayTotal: atomic.LoadInt64(&s.onewayTotal),
		OnewayError: atomic.LoadInt64(&s.onewayError),

		InternalErrors: atomic.LoadInt64(&s.internalErrors),

		PendingCurrent: pendingCurrent,
		PendingMax:     pendingMax,
	}
}

// Reset 重置统计信息。
func (s *RpcStats) Reset() {
	atomic.StoreInt64(&s.requestsTotal, 0)
	atomic.StoreInt64(&s.requestsSuccess, 0)
	atomic.StoreInt64(&s.requestsError, 0)
	atomic.StoreInt64(&s.requestsTimeout, 0)
	atomic.StoreInt64(&s.requestsCanceled, 0)

	atomic.StoreInt64(&s.onewayTotal, 0)
	atomic.StoreInt64(&s.onewayError, 0)

	atomic.StoreInt64(&s.internalErrors, 0)
}

//----------------------------------------------------------

// MetricsFromSnapshot 将 RpcStatsSnapshot 转换为可用于 metrics 导出的键值对。
//
// - namespace 用于区分不同实例/模块（例如 "gate_rpc"、"logic_rpc"），为空则不加前缀。
// - 返回的 key 采用 "<namespace>.<name>" 或仅 "<name>" 形式，值统一为 float64，方便接入 Prometheus、StatsD 等。
//
// 约定的度量名称（未加 namespace）：
//   - kkrpc_requests_total
//   - kkrpc_requests_success_total
//   - kkrpc_requests_error_total
//   - kkrpc_requests_timeout_total
//   - kkrpc_requests_canceled_total
//   - kkrpc_oneway_total
//   - kkrpc_oneway_error_total
//   - kkrpc_internal_errors_total
//   - kkrpc_pending_current
//   - kkrpc_pending_max
func MetricsFromSnapshot(namespace string, snap RpcStatsSnapshot) map[string]float64 {
	prefix := ""
	if namespace != "" {
		prefix = namespace + "."
	}

	m := make(map[string]float64, 10)
	m[prefix+"kkrpc_requests_total"] = float64(snap.RequestsTotal)
	m[prefix+"kkrpc_requests_success_total"] = float64(snap.RequestsSuccess)
	m[prefix+"kkrpc_requests_error_total"] = float64(snap.RequestsError)
	m[prefix+"kkrpc_requests_timeout_total"] = float64(snap.RequestsTimeout)
	m[prefix+"kkrpc_requests_canceled_total"] = float64(snap.RequestsCanceled)

	m[prefix+"kkrpc_oneway_total"] = float64(snap.OnewayTotal)
	m[prefix+"kkrpc_oneway_error_total"] = float64(snap.OnewayError)

	m[prefix+"kkrpc_internal_errors_total"] = float64(snap.InternalErrors)

	m[prefix+"kkrpc_pending_current"] = float64(snap.PendingCurrent)
	m[prefix+"kkrpc_pending_max"] = float64(snap.PendingMax)

	return m
}

