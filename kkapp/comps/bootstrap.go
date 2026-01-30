package comps

import (
	"fmt"

	"github.com/vvisun/kkdg/kkapp/comps/ccgame"
	"github.com/vvisun/kkdg/kkapp/comps/ccgate"
	"github.com/vvisun/kkdg/kkapp/component"
)

// AddNodeComponents adds default components based on node type.
//
// - gate node: adds `ccgate` component (TCP/WS listener + forwarder)
// - logic node: adds `ccgame` component (cluster receiver)
func AddNodeComponents(app component.IApplication, gateOpt ccgate.Option) error {
	if app == nil {
		return fmt.Errorf("comps: nil application")
	}
	switch app.GetNodeType() {
	case NodeTypeGate:
		return app.AddComponent(ccgate.NewGateComponent(gateOpt))
	case NodeTypeLogic:
		return app.AddComponent(ccgame.NewGameComponent())
	default:
		return fmt.Errorf("comps: unsupported node type: %s", app.GetNodeType())
	}
}

