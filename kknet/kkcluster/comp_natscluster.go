package kkcluster

import "github.com/vvisun/kkdg/kkapp/component"

type CompNatsCluster struct {
	component.Component
	cluster ICluster
}

var _ component.IComponent = (*CompNatsCluster)(nil)

func NewCompNatsCluster(cluster ICluster) *CompNatsCluster {
	return &CompNatsCluster{
		cluster: cluster,
	}
}

func (slf *CompNatsCluster) GetID() string {
	return "natscluster"
}

var _ component.IComponent = (*CompNatsCluster)(nil)

func (slf *CompNatsCluster) Init() error {
	return slf.cluster.Init()
}

func (slf *CompNatsCluster) Start() error {
	return nil
}

func (slf *CompNatsCluster) Stop() error {
	slf.cluster.Stop()
	return nil
}

func (slf *CompNatsCluster) GraceStop() error {
	slf.cluster.Stop()
	return nil
}
