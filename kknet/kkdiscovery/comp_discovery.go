package kkdiscovery

import "github.com/vvisun/kkdg/kkapp/component"

type compDiscovery struct {
	component.Component
	discovery IDiscovery
}

var _ component.IComponent = (*compDiscovery)(nil)

func NewCompDiscovery(discovery IDiscovery) *compDiscovery {
	return &compDiscovery{
		discovery: discovery,
	}
}

func (slf *compDiscovery) GetID() string {
	return slf.discovery.Name()
}

func (slf *compDiscovery) Init() error {
	return nil
}

func (slf *compDiscovery) Start() error {
	return slf.discovery.Start()
}

func (slf *compDiscovery) Stop() error {
	return slf.discovery.Stop()
}
