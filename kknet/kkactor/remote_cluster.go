package kkactor

import (
	"fmt"
	"time"

	"github.com/vvisun/kkdg/kknet/kkcluster"
)

// ClusterRemote adapts kkcluster.ICluster to the Remote interface.
type ClusterRemote struct {
	nodeID  string
	cluster kkcluster.ICluster
}

// NewClusterRemote creates a remote transport based on kkcluster.
func NewClusterRemote(nodeID string, cluster kkcluster.ICluster) *ClusterRemote {
	return &ClusterRemote{
		nodeID:  nodeID,
		cluster: cluster,
	}
}

// NodeID returns the local node id.
func (r *ClusterRemote) NodeID() string {
	if r == nil {
		return ""
	}
	return r.nodeID
}

// Publish sends a fire-and-forget message to a node.
func (r *ClusterRemote) Publish(nodeID string, data []byte) error {
	if r == nil || r.cluster == nil {
		return ErrRemoteNotConfigured
	}
	packet := &kkcluster.ClusterPacket{
		BuildTime:  time.Now().UnixMilli(),
		SourcePath: r.nodeID,
		TargetPath: nodeID,
		FuncName:   remoteFuncName,
		ArgBytes:   data,
	}
	return r.cluster.PublishRemote(nodeID, packet)
}

// Request sends a request and waits for a response.
func (r *ClusterRemote) Request(nodeID string, data []byte, timeout ...time.Duration) ([]byte, error) {
	if r == nil || r.cluster == nil {
		return nil, ErrRemoteNotConfigured
	}
	packet := &kkcluster.ClusterPacket{
		BuildTime:  time.Now().UnixMilli(),
		SourcePath: r.nodeID,
		TargetPath: nodeID,
		FuncName:   remoteFuncName,
		ArgBytes:   data,
	}
	resp, code := r.cluster.RequestRemote(nodeID, packet, timeout...)
	if code != kkcluster.ClusterErrorCodeSuccess {
		return nil, fmt.Errorf("%w: %d", ErrRemoteRequestFailed, code)
	}
	return resp, nil
}

// SetPublishHandler sets the handler for publish messages.
func (r *ClusterRemote) SetPublishHandler(handler func(sourceNodeID string, data []byte)) {
	if r == nil || r.cluster == nil {
		return
	}
	r.cluster.SetPublishHandler(func(nodeID string, packet *kkcluster.ClusterPacket) {
		if handler == nil || packet == nil || packet.FuncName != remoteFuncName {
			return
		}
		handler(nodeID, packet.ArgBytes)
	})
}

// SetRequestHandler sets the handler for request messages.
func (r *ClusterRemote) SetRequestHandler(handler func(sourceNodeID string, data []byte) ([]byte, error)) {
	if r == nil || r.cluster == nil {
		return
	}
	r.cluster.SetRequestHandler(func(req *kkcluster.ClusterRequest) (*kkcluster.ClusterResponse, error) {
		if req == nil || req.Packet == nil || req.Packet.FuncName != remoteFuncName {
			return &kkcluster.ClusterResponse{
				RequestID: req.RequestID,
				Code:      int32(kkcluster.ClusterErrorCodeInvalidRequest),
			}, nil
		}
		if handler == nil {
			return &kkcluster.ClusterResponse{
				RequestID: req.RequestID,
				Code:      int32(kkcluster.ClusterErrorCodeFail),
			}, nil
		}
		data, err := handler(req.SourceNodeID, req.Packet.ArgBytes)
		if err != nil {
			return &kkcluster.ClusterResponse{
				RequestID: req.RequestID,
				Code:      int32(kkcluster.ClusterErrorCodeFail),
			}, nil
		}
		return &kkcluster.ClusterResponse{
			RequestID: req.RequestID,
			Code:      int32(kkcluster.ClusterErrorCodeSuccess),
			Data:      data,
		}, nil
	})
}
