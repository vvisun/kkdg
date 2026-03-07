package examapp

import (
	"time"

	"github.com/vvisun/kkdg/kkapp"
)

const (
	NatsURL      = "nats://127.0.0.1:4222"
	UseTransType = kkapp.TransTypeNats
	GateTCPAddr  = "127.0.0.1:19090"
	GateWSAddr   = "127.0.0.1:19091"
	RpcAddr      = "127.0.0.1:19092"
)

const (
	ClientConnNum      = 2000
	ClientConnDelay    = 5 * time.Millisecond
	ClientSendInterval = 200 * time.Millisecond
)
