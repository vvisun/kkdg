package faultreport

// EFaultAction 是Application对故障的处理动作。
// 注意：这里的动作是Application对故障的处理动作，而不是组件对故障的处理动作。
//  建议的处理动作是：优先停止应用，其次是停止组件。并不推荐重启动作。
//  因为重启后，可能还会再次故障，而任何故障发生时，基本都是应用bug了，应该停服维护再重启。
//  否则可能反复故障重启，将未知的风险无限扩散，影响面无限放大。
type EFaultAction int

const (
	FaultActionStopApp     EFaultAction = iota // 停止应用
	FaultActionStopComp                        // 停止组件
	actionCount
)

func (slf EFaultAction) String() string {
	switch slf {
	case FaultActionStopApp:
		return "stop_app"
	case FaultActionStopComp:
		return "stop_comp"
	}
	return "unknown"
}

func IsValidFaultAction(action EFaultAction) bool {
	return action >= FaultActionStopApp && action < actionCount
}
