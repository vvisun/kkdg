package kkmetrics

import (
	"context"
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
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
