package kkdiscovery

type (
	// MemberInfo 成员信息（用于序列化）
	MemberInfo struct {
		NodeID   string            `json:"nodeID"`   //节点ID, 用于标识一个节点。世界唯一。
		NodeType string            `json:"nodeType"` //节点类型, 用于标识一个节点的类型。如：gate、game、login等。
		Address  string            `json:"address"`  //地址, 用于连接， 直接填[tcp://127.0.0.1:8080]即可
		Weight   int               `json:"weight"`   //权重, 用于负载均衡， 直接填连接数即可
		Status   int               `json:"status"`   //0: online, 1: offline
		Settings map[string]string `json:"settings"` //节点配置参数。如：{"log_level": "debug"}
	}

	// DiscoveryRequest 发现请求
	DiscoveryRequest struct {
		RequesterID string `json:"requesterID"`
	}
)
