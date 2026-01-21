package ccgate

import (
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet/kkpacket"
)

// codec for the gate
var codec kkpacket.PacketCodec

// GateComponent is a component that provides a gate for the application.
type GateComponent struct {
	component.Component
}
