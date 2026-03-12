package kkmetrics

import (
	"context"

	"go.opentelemetry.io/otel/metric"
)

// InitClusterMetrics registers observable gauges for cluster metrics.
// 同样通过 CollectClusterMetrics 触发事件，由集群模块回填 MetricsEventData，
// 然后根据返回的 key 自动生成指标，避免在这里手写 metric 名称。
func InitClusterMetrics(ctx context.Context) error {
	if Meter == nil {
		return nil
	}

	// 先触发一次事件，拿到当前快照中的所有 key，用于生成指标元数据
	e := collectClusterMetrics("")
	if e == nil || len(e.Metrics) == 0 {
		return nil
	}

	gauges := make(map[string]metric.Float64ObservableGauge, len(e.Metrics))
	objs := make([]metric.Observable, 0, len(e.Metrics))

	for name := range e.Metrics {
		g, err := Meter.Float64ObservableGauge(
			name,
			metric.WithDescription("auto-generated cluster metric: "+name),
		)
		if err != nil {
			return err
		}
		gauges[name] = g
		objs = append(objs, g)
	}

	_, err := Meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		snap := collectClusterMetrics("")
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
