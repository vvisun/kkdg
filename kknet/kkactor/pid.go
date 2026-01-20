package kkactor

import "fmt"

// PID identifies an actor in a system.
type PID struct {
	id     string
	system *ActorSystem
	nodeID string
}

// NewRemotePID creates a pid targeting a remote node.
func NewRemotePID(nodeID, id string) *PID {
	return &PID{id: id, nodeID: nodeID}
}

// ID returns the pid's unique id.
func (p *PID) ID() string {
	if p == nil {
		return ""
	}
	return p.id
}

// System returns the owning actor system.
func (p *PID) System() *ActorSystem {
	if p == nil {
		return nil
	}
	return p.system
}

// NodeID returns the node id for remote actors.
func (p *PID) NodeID() string {
	if p == nil {
		return ""
	}
	if p.nodeID != "" {
		return p.nodeID
	}
	if p.system != nil {
		return p.system.nodeID
	}
	return ""
}

// IsRemote reports whether the pid targets a remote node.
func (p *PID) IsRemote() bool {
	if p == nil {
		return false
	}
	if p.system != nil && p.nodeID == "" {
		return false
	}
	if p.nodeID == "" {
		return false
	}
	if p.system == nil {
		return true
	}
	return p.nodeID != p.system.nodeID
}

func (p *PID) String() string {
	if p == nil {
		return "<nil>"
	}
	if p.nodeID != "" {
		return fmt.Sprintf("pid(%s@%s)", p.id, p.nodeID)
	}
	return fmt.Sprintf("pid(%s)", p.id)
}
