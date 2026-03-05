package kkdiscovery

import "testing"

func TestMetricsFromSnapshot_Basic(t *testing.T) {
	snap := DiscoveryStatsSnapshot{
		MemberCount:        3,
		MembersAdded:       5,
		MembersRemoved:     2,
		HeartbeatsSent:     10,
		HeartbeatsReceived: 9,
		Errors:             1,
		Reconnects:         4,
		IsConnected:        true,
	}

	m := MetricsFromSnapshot("discovery", snap)

	tests := map[string]float64{
		"discovery.discovery_member_count":            3,
		"discovery.discovery_members_added_total":     5,
		"discovery.discovery_members_removed_total":   2,
		"discovery.discovery_heartbeats_sent_total":   10,
		"discovery.discovery_heartbeats_received_total": 9,
		"discovery.discovery_errors_total":            1,
		"discovery.discovery_reconnects_total":        4,
		"discovery.discovery_is_connected":            1,
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
	snap := DiscoveryStatsSnapshot{
		MemberCount:        1,
		MembersAdded:       1,
		MembersRemoved:     0,
		HeartbeatsSent:     2,
		HeartbeatsReceived: 2,
		Errors:             0,
		Reconnects:         0,
		IsConnected:        false,
	}

	m := MetricsFromSnapshot("", snap)

	if v := m["discovery_member_count"]; v != 1 {
		t.Fatalf("discovery_member_count = %v, want 1", v)
	}
	if v := m["discovery_is_connected"]; v != 0 {
		t.Fatalf("discovery_is_connected = %v, want 0", v)
	}
}

