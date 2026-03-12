package kkmetrics

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

// InitDiscoveryMetrics registers observable gauges for discovery metrics.
// 它通过 CollectDiscoveryMetrics (event-based) 拉取最新快照，根据返回的 key 动态生成指标，
// 避免在这里手写/维护具体的 metric 名称。
func InitDiscoveryMetrics(ctx context.Context) error {
	if Meter == nil {
		return nil
	}

	// 先触发一次事件，拿到当前快照中的所有 key，用于生成指标元数据
	e := collectDiscoveryMetrics("")
	if e == nil || len(e.Metrics) == 0 {
		// 没有监听者或暂时没有数据，直接返回即可，后面可以在其他地方再次调用 InitDiscoveryMetrics
		return nil
	}

	gauges := make(map[string]metric.Float64ObservableGauge, len(e.Metrics))
	objs := make([]metric.Observable, 0, len(e.Metrics))

	for name := range e.Metrics {
		g, err := Meter.Float64ObservableGauge(
			name,
			metric.WithDescription("auto-generated discovery metric: "+name),
		)
		if err != nil {
			return err
		}
		gauges[name] = g
		objs = append(objs, g)
	}

	_, err := Meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		snap := collectDiscoveryMetrics("")
		if snap == nil || snap.Metrics == nil {
			return nil
		}
		for name, gauge := range gauges {
			if v, ok := snap.Metrics[name]; ok {
				observer.ObserveFloat64(gauge, v)
			}
		}
		return nil
	}, objs...)
	return err
}
