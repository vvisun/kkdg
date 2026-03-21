package component

type ComponentState = int64

const (
	ComponentStateNone     ComponentState = iota //组件未初始化
	ComponentStateStarting                       //组件启动中
	ComponentStateStarted                        //组件已启动
	ComponentStateStopping                       //组件停止中
	ComponentStateStopped                        //组件已停止
)

var stateNameMap = map[ComponentState]string{
	ComponentStateNone:     "none",
	ComponentStateStarting: "starting",
	ComponentStateStarted:  "started",
	ComponentStateStopping: "stopping",
	ComponentStateStopped:  "stopped",
}

func getStateName(state ComponentState) string {
	return stateNameMap[state]
}
