package kkdiscovery

type (
	// IDiscovery 发现服务接口
	IDiscovery interface {
		Name() string                                                 // 发现服务名称
		Start() error                                                 // 启动
		Stop() error                                                  // 停止
		MemberCount() int                                             // 获取成员数量
		Range(fn func(nodeID string, member IMember) bool)            // 遍历成员, fn 返回 false 时停止遍历
		ListByType(nodeType string, filterNodeID ...string) []IMember // 根据节点类型获取列表
		Random(nodeType string) (IMember, bool)                       // 根据节点类型随机一个
		GetType(nodeID string) (nodeType string, err error)           // 根据节点id获取类型
		GetMember(nodeID string) (member IMember, found bool)         // 获取成员
		OnAddMember(listener MemberListener)                          // watcher 添加成员监听函数
		OnRemoveMember(listener MemberListener)                       // watcher 移除成员监听函数
		Stats() DiscoveryStatsSnapshot                                // 获取统计信息
	}

	// IMember 成员接口
	IMember interface {
		GetNodeID() string                  // 节点ID。必须唯一。
		GetNodeType() string                // 节点类型。如：gate、game、login等
		GetAddress() string                 // 节点地址。如：127.0.0.1:8080
		GetSetting(k string) (string, bool) // 额外数据，可以为空。
		GetWeight() int                     // 获取权重
		SetWeight(weight int)               // 设置权重
	}

	// MemberListener 成员增、删监听函数
	MemberListener func(member IMember)
)
