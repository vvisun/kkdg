package kkdiscovery

type (
	// MemberInfo 成员信息（用于序列化）
	MemberInfo struct {
		NodeID     string `json:"nodeID"`     //节点ID, 用于标识一个节点。世界唯一。
		NodeType   string `json:"nodeType"`   //节点类型, 用于标识一个节点的类型。如：gate、game、login等。
		Address    string `json:"address"`    //网关transport地址, 网关与逻辑服之间的转发通道地址。
		RpcAddress string `json:"rpcAddress"` //actor通信rpc server地址, 用于actor与actor之间的通信。
		Weight     int    `json:"weight"`     //权重, 用于负载均衡。eg: 连接数
		Status     int    `json:"status"`     //NodeStatusOnline(0), NodeStatusOffline(1)
	}

	// DiscoveryRequest 发现请求
	DiscoveryRequest struct {
		RequesterID string `json:"requesterID"`
	}
)
