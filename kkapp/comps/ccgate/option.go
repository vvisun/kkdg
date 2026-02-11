package ccgate

import "github.com/vvisun/kkdg/kknet/kkpacket"

// Option configures the gate component.
type Option struct {
	TCPAddr string
	WSAddr  string

	// NatsURL is the NATS server url used by discovery/cluster.
	// If empty, it falls back to nodeInfo setting "nats_url", then default "nats://127.0.0.1:4222".
	NatsURL string

	// LogicNodeType is the target node type for game logic nodes.
	// If empty, defaults to "logic".
	LogicNodeType string

	// MsgRouter for resolving msgID to route. If nil, uses NewMsgRouter().
	MsgRouter *kkpacket.MsgRouter
}
