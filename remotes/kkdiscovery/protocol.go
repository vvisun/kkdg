package kkdiscovery

type (
	// MemberInfo 成员信息（用于序列化）
	MemberInfo struct {
		NodeID     string            `json:"nodeID"`     //节点ID, 用于标识一个节点。世界唯一。
		NodeType   string            `json:"nodeType"`   //节点类型, 用于标识一个节点的类型。如：gate、game、login等。
		Address    string            `json:"address"`    //地址, 用于连接。eg: tcp://127.0.0.1:8080, ws://127.0.0.1:8080
		RpcAddress string            `json:"rpcAddress"` //rpc地址, 用于rpc通信。eg: 127.0.0.1:8080
		Weight     int               `json:"weight"`     //权重, 用于负载均衡。eg: 连接数
		Status     int               `json:"status"`     //NodeStatusOnline(0), NodeStatusOffline(1)
		Settings   map[string]string `json:"settings"`   //可选。节点配置参数。eg: {"log_level": "debug"}
	}

	// DiscoveryRequest 发现请求
	DiscoveryRequest struct {
		RequesterID string `json:"requesterID"`
	}
)
