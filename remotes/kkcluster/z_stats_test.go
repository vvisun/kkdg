package kkcluster

import "testing"

func TestMetricsFromSnapshot_Basic(t *testing.T) {
	snap := ClusterStatsSnapshot{
		PublishSent:      10,
		PublishReceived:  9,
		RequestSent:      7,
		RequestReceived:  6,
		ResponseSent:     5,
		ResponseReceived: 4,
		SentBytes:        1234,
		ReceivedBytes:    5678,
		Errors:           2,
		Reconnects:       3,
		IsConnected:      true,
	}

	m := MetricsFromSnapshot("cluster", snap)

	tests := map[string]float64{
		"cluster.cluster_publish_sent_total":      10,
		"cluster.cluster_publish_received_total":  9,
		"cluster.cluster_request_sent_total":      7,
		"cluster.cluster_request_received_total":  6,
		"cluster.cluster_response_sent_total":     5,
		"cluster.cluster_response_received_total": 4,
		"cluster.cluster_sent_bytes_total":        1234,
		"cluster.cluster_received_bytes_total":    5678,
		"cluster.cluster_errors_total":            2,
		"cluster.cluster_reconnects_total":        3,
		"cluster.cluster_is_connected":            1,
	}

	for key, want := range tests {
		got, ok := m[key]
		if !ok {
			t.Fatalf("metrics missing key %q", key)
		}
		if got != want {
			t.Fatalf("metrics[%q] = %v, want %v", key, got, want)
		}
	}
}

func TestMetricsFromSnapshot_NoNamespace(t *testing.T) {
	snap := ClusterStatsSnapshot{
		PublishSent:      1,
		PublishReceived:  1,
		RequestSent:      1,
		RequestReceived:  1,
		ResponseSent:     1,
		ResponseReceived: 1,
		SentBytes:        10,
		ReceivedBytes:    20,
		Errors:           0,
		Reconnects:       0,
		IsConnected:      false,
	}

	m := MetricsFromSnapshot("", snap)

	if v := m["cluster_publish_sent_total"]; v != 1 {
		t.Fatalf("cluster_publish_sent_total = %v, want 1", v)
	}
	if v := m["cluster_is_connected"]; v != 0 {
		t.Fatalf("cluster_is_connected = %v, want 0", v)
	}
}
