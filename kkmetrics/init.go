package kkmetrics

import (
	"context"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/vvisun/kkdg/utils/kklog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// Meter is the global metric.Meter used by kkdg.
var Meter metric.Meter

// Init initializes the global MeterProvider and exposes /metrics on the given address.
// Example: addr = ":2112"
func Init(ctx context.Context, addr string) error {
	exp, err := prometheus.New()
	if err != nil {
		return err
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exp),
	)
	otel.SetMeterProvider(mp)
	Meter = mp.Meter("github.com/vvisun/kkdg")

	// expose /metrics
	go func() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())
		if err := http.ListenAndServe(addr, mux); err != nil {
			log.Printf("metrics http server error: %v", err)
		}
	}()

	return nil
}

func AutoInit(ctx context.Context, addr string) {
	modInits := []func(ctx context.Context) error{
		InitClusterMetrics,
		InitDiscoveryMetrics,
	}
	curInitIndex := 0

	go func() {
		for {
			if Init(ctx, addr) == nil {
				// 成功初始化指标，退出循环
				break
			}
			// 初始化失败，重试
			time.Sleep(1 * time.Second)
		}

		kklog.Infof("metrics http server started on %s", addr)

		// 初始化各个模块的指标
		for {
			if curInitIndex >= len(modInits) {
				// 所有模块的指标都初始化成功，退出循环
				break
			}
			curFn := modInits[curInitIndex]
			if curFn(ctx) == nil {
				// 成功初始化一个模块的指标，继续初始化下一个模块
				curInitIndex++
				time.Sleep(50 * time.Millisecond)
				continue
			} else {
				// 初始化失败，重试
				time.Sleep(1 * time.Second)
				continue
			}
		}
	}()
}

// InitClusterMetrics registers observable gauges for cluster metrics.
// 同样通过 CollectClusterMetrics 触发事件，由集群模块回填 MetricsEventData，
// 然后根据返回的 key 自动生成指标，避免在这里手写 metric 名称。
func InitClusterMetrics(ctx context.Context) error {
	return initTrigger(ctx, "cluster", collectClusterMetrics)
}

// InitDiscoveryMetrics registers observable gauges for discovery metrics.
// 它通过 CollectDiscoveryMetrics (event-based) 拉取最新快照，根据返回的 key 动态生成指标，
// 避免在这里手写/维护具体的 metric 名称。
func InitDiscoveryMetrics(ctx context.Context) error {
	return initTrigger(ctx, "discovery", collectDiscoveryMetrics)
}

func initTrigger(ctx context.Context, namespace string, collectFunc collectFunc) error {
	if Meter == nil {
		return nil
	}

	// 先触发一次事件，拿到当前快照中的所有 key，用于生成指标元数据
	e := collectFunc(namespace)
	if e == nil || len(e.Metrics) == 0 {
		// 没有监听者或暂时没有数据，直接返回即可，后面可以在其他地方再次调用 InitXXXXMetrics
		return nil
	}

	gauges := make(map[string]metric.Float64ObservableGauge, len(e.Metrics))
	objs := make([]metric.Observable, 0, len(e.Metrics))

	for name := range e.Metrics {
		g, err := Meter.Float64ObservableGauge(
			name,
			metric.WithDescription("auto-generated "+namespace+" metric: "+name),
		)
		if err != nil {
			return err
		}
		gauges[name] = g
		objs = append(objs, g)
	}

	_, err := Meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		snap := collectFunc(namespace)
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

	if err == nil {
		kklog.Infof("metrics %s initialized", namespace)
	}

	return err
}
