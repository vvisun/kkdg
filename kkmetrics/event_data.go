package kkmetrics

import "github.com/vvisun/kkdg/utils/kkevent"

// 向各模块发出的请求事件，用于收集各模块的metrics数据
const (
	// 集群metrics事件
	EventClusterMetrics = "cluster_metrics"
	// 发现metrics事件
	EventDiscoveryMetrics = "discovery_metrics"
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
	kkevent.Publish(EventClusterMetrics, e)
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
	kkevent.Publish(EventDiscoveryMetrics, e)
	return e
}
