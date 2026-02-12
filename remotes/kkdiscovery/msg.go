package kkdiscovery

type (
	// MemberInfo 成员信息（用于序列化）
	MemberInfo struct {
		NodeID   string            `json:"nodeID"`
		NodeType string            `json:"nodeType"`
		Address  string            `json:"address"`
		Settings map[string]string `json:"settings"`
	}

	// DiscoveryRequest 发现请求
	DiscoveryRequest struct {
		RequesterID string `json:"requesterID"`
	}
)
