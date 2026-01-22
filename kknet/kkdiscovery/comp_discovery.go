package kkdiscovery

import "github.com/vvisun/kkdg/kkapp/component"

type CompDiscovery struct {
	component.Component
	discovery IDiscovery
}

var _ component.IComponent = (*CompDiscovery)(nil)

func NewCompDiscovery(discovery IDiscovery) *CompDiscovery {
	return &CompDiscovery{
		discovery: discovery,
	}
}

func (slf *CompDiscovery) GetID() string {
	return slf.discovery.Name()
}

func (slf *CompDiscovery) Init() error {
	return nil
}

func (slf *CompDiscovery) Start() error {
	return slf.discovery.Start()
}

func (slf *CompDiscovery) Stop() error {
	return slf.discovery.Stop()
}

func (slf *CompDiscovery) GraceStop() error {
	return slf.Stop()
}
