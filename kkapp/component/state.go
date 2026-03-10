package component

type ComponentState = int64

const (
	ComponentStateNone     ComponentState = iota //组件未初始化
	ComponentStateStarting                       //组件启动中
	ComponentStateStarted                        //组件已启动
	ComponentStateStoping                        //组件停止中
	ComponentStateStoped                         //组件已停止
)

var stateNameMap = map[ComponentState]string{
	ComponentStateNone:     "none",
	ComponentStateStarting: "starting",
	ComponentStateStarted:  "started",
	ComponentStateStoping:  "stoping",
	ComponentStateStoped:   "stoped",
}

func GetStateName(state ComponentState) string {
	return stateNameMap[state]
}
