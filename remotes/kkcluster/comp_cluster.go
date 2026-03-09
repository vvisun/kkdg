package kkcluster

import "github.com/vvisun/kkdg/kkapp/component"

type compCluster struct {
	component.Component
	cluster ICluster
}

var _ component.IComponent = (*compCluster)(nil)

func NewCompNatsCluster(cluster ICluster) *compCluster {
	return &compCluster{
		cluster: cluster,
	}
}

func (slf *compCluster) GetCompName() string {
	return "cluster_" + slf.GetApplication().GetNodeInfo().GetNodeId()
}

var _ component.IComponent = (*compCluster)(nil)

func (slf *compCluster) Init() error {
	return slf.cluster.Start()
}

func (slf *compCluster) Start() error {
	return nil
}

func (slf *compCluster) Stop() error {
	slf.cluster.Stop()
	return nil
}
