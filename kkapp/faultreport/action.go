package faultreport

// EFaultAction 是Application对故障的处理动作。
type EFaultAction int

const (
	FaultActionStopApp     EFaultAction = iota // 停止应用
	FaultActionRestartApp                      // 重启应用
	FaultActionRestartComp                     // 重启组件
	FaultActionStopComp                        // 停止组件
	actionCount
)

func (slf EFaultAction) String() string {
	switch slf {
	case FaultActionRestartComp:
		return "restart_comp"
	case FaultActionStopComp:
		return "stop_comp"
	case FaultActionRestartApp:
		return "restart_app"
	}
	return "unknown"
}

func IsValidFaultAction(action EFaultAction) bool {
	return action >= FaultActionStopApp && action < actionCount
}
