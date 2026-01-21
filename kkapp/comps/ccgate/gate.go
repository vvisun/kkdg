package ccgate

import (
	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet/kkpacket"
)

// codec for the gate
var codec kkpacket.PacketCodec

// 网关服
type GateComponent struct {
	component.Component
}

func (slf *GateComponent) GetID() string {
	return "gate"
}

var _ component.IComponent = (*GateComponent)(nil)

func (slf *GateComponent) Init() error {
	return nil
}

func (slf *GateComponent) Start() error {
	return nil
}

func (slf *GateComponent) Stop() error {
	return nil
}

func (slf *GateComponent) startTCPServer() error {
	return nil
}

func (slf *GateComponent) startWSServer() error {
	return nil
}
