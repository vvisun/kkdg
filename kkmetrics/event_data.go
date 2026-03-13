package kkmetrics

import (
	"context"

	"github.com/vvisun/kkdg/utils/kkevent"
)

// 向各模块发出的请求事件，用于收集各模块的metrics数据
const (
	// 集群metrics事件
	EventClusterMetrics = "cluster_metrics"
	// 发现metrics事件
	EventDiscoveryMetrics = "discovery_metrics"
	// rpc客户端metrics事件
	EventRpcClientMetrics = "rpc_client_metrics"
	// rpc服务端metrics事件
	EventRpcServerMetrics = "rpc_server_metrics"
)

// MetricsEventData 是向各模块发出的请求事件的数据结构
type MetricsEventData struct {
	EventType string             // 回填事件类型，用于标识是哪个模块的metrics数据
	Namespace string             // metrics数据所属的命名空间
	Metrics   map[string]float64 // metrics数据
}

type collectFunc func(namespace string) *MetricsEventData

// collectClusterMetrics 通过事件总线请求集群模块填充 metrics 数据。
// 监听方（例如应用中持有的 kkcluster.ICluster 实例）需要订阅 EventClusterMetrics，
// 在回调中根据 Namespace 调用 kkcluster.MetricsFromSnapshot 并回填到 e.Metrics。
func collectClusterMetrics(namespace string) *MetricsEventData {
	e := &MetricsEventData{
		EventType: EventClusterMetrics,
		Namespace: namespace,
		Metrics:   make(map[string]float64),
	}
	kkevent.GlobalBus.Publish(EventClusterMetrics, e)
	return e
}

// collectDiscoveryMetrics 通过事件总线请求服务发现模块填充 metrics 数据。
// 监听方需要订阅 EventDiscoveryMetrics，在回调中调用 kkdiscovery.MetricsFromSnapshot 并回填。
func collectDiscoveryMetrics(namespace string) *MetricsEventData {
	e := &MetricsEventData{
		EventType: EventDiscoveryMetrics,
		Namespace: namespace,
		Metrics:   make(map[string]float64),
	}
	kkevent.GlobalBus.Publish(EventDiscoveryMetrics, e)
	return e
}

// collectRpcClientMetrics 通过事件总线请求rpc客户端模块填充 metrics 数据。
// 监听方需要订阅 EventRpcClientMetrics，在回调中调用 kkrpc.MetricsFromSnapshot 并回填。
func collectRpcClientMetrics(namespace string) *MetricsEventData {
	e := &MetricsEventData{
		EventType: EventRpcClientMetrics,
		Namespace: namespace,
		Metrics:   make(map[string]float64),
	}
	kkevent.GlobalBus.Publish(EventRpcClientMetrics, e)
	return e
}

// collectRpcServerMetrics 通过事件总线请求rpc服务端模块填充 metrics 数据。
// 监听方需要订阅 EventRpcServerMetrics，在回调中调用 kkrpc.MetricsFromSnapshot 并回填。
func collectRpcServerMetrics(namespace string) *MetricsEventData {
	e := &MetricsEventData{
		EventType: EventRpcServerMetrics,
		Namespace: namespace,
		Metrics:   make(map[string]float64),
	}
	kkevent.GlobalBus.Publish(EventRpcServerMetrics, e)
	return e
}

// initClusterMetrics registers observable gauges for cluster metrics.
// 同样通过 CollectClusterMetrics 触发事件，由集群模块回填 MetricsEventData，
// 然后根据返回的 key 自动生成指标，避免在这里手写 metric 名称。
func initClusterMetrics(ctx context.Context) error {
	return initTrigger(ctx, "cluster", collectClusterMetrics)
}

// initDiscoveryMetrics registers observable gauges for discovery metrics.
// 它通过 CollectDiscoveryMetrics (event-based) 拉取最新快照，根据返回的 key 动态生成指标，
// 避免在这里手写/维护具体的 metric 名称。
func initDiscoveryMetrics(ctx context.Context) error {
	return initTrigger(ctx, "discovery", collectDiscoveryMetrics)
}

// initRpcClientMetrics registers observable gauges for rpc client metrics.
func initRpcClientMetrics(ctx context.Context) error {
	return initTrigger(ctx, "rpc_client", collectRpcClientMetrics)
}

// initRpcServerMetrics registers observable gauges for rpc server metrics.
func initRpcServerMetrics(ctx context.Context) error {
	return initTrigger(ctx, "rpc_server", collectRpcServerMetrics)
}
