package faultreport

// EFaultAction 是Application对故障的处理动作。
type EFaultAction int

const (
	FaultActionStopApp     EFaultAction = iota // 停止应用
	FaultActionStopComp                        // 停止组件
	FaultActionRestartComp                     // 重启组件
	actionCount
)

func (slf EFaultAction) String() string {
	switch slf {
	case FaultActionStopApp:
		return "stop_app"
	case FaultActionStopComp:
		return "stop_comp"
	case FaultActionRestartComp:
		return "restart_comp"
	}
	return "unknown"
}

func IsValidFaultAction(action EFaultAction) bool {
	return action >= FaultActionStopApp && action < actionCount
}
