package kkdiscovery

const (
	NodeStatusOnline  = 0 // 在线
	NodeStatusOffline = 1 // 离线
)

const (
	// 发现服务统计事件, 用于得知成员的 在线数量，状态 等统计信息。
	// 需要统计时服务发现会发布该事件，对应的模块监听本事件并填充数据即可。
	EventDiscoveryStats = "kkdiscovery_stats"
)

type DiscoveryStatsEvent struct {
	OnlineCount int // 在线玩家数量
	Status      int // 状态：NodeStatusOnline(0), NodeStatusOffline(1)
}

type (
	// MemberListener 成员增、删监听函数
	MemberListener func(member IMember)

	// IInnerMemberMgr 内部成员管理器接口。不对外使用，防止污染成员
	IInnerMemberMgr interface {
		// 添加成员，true时为新增，false时为更新
		AddMember(info *MemberInfo) (IMember, bool)
		// 删除成员，true时成员存在，false时成员不存在
		RemoveMember(nodeID string) bool
	}

	// MemberMgr 成员管理器接口
	IMemberMgr interface {
		// 获取成员数量
		MemberCount() int
		// 遍历成员, fn 返回 false 时停止遍历
		Range(fn func(nodeID string, member IMember) bool)

		// 节点类型为nodeType的成员数量
		CountOfType(nodeType string) int
		// 遍历节点类型为nodeType的成员, fn 返回 false 时停止遍历
		RangeType(nodeType string, fn func(nodeID string, member IMember) bool)

		// 根据节点id获取成员类型
		GetType(nodeID string) (string, error)
		// 根据节点id获取成员
		GetMember(nodeID string) (IMember, bool)

		// 监听添加成员
		ObserveAddMember(listener MemberListener)
		// 监听移除成员
		ObserveRemoveMember(listener MemberListener)
	}

	// IDiscovery 发现服务接口
	IDiscovery interface {
		Name() string                  // 发现服务名称
		Start() error                  // 启动
		Stop() error                   // 停止
		Stats() DiscoveryStatsSnapshot // 获取统计信息
		GetMemberMgr() IMemberMgr      // 获取成员管理器
		IsRunning() bool               // 是否已启动
	}

	// IMember 成员接口
	IMember interface {
		GetNodeID() string     // 节点ID。必须唯一。
		GetNodeType() string   // 节点类型。如：gate、game、login等
		GetAddress() string    // 节点地址。如：127.0.0.1:8080
		GetRpcAddress() string // rpc监听地址。如：127.0.0.1:8080
		GetWeight() int        // 获取权重
		GetStatus() int        // 获取状态
	}
)
