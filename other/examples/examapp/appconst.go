package examapp

import (
	"time"

	"github.com/vvisun/kkdg/kkapp"
)

// 默认值；通过 ParseFlags() 可由命令行参数覆盖
var (
	NatsURL      = "nats://127.0.0.1:4222"
	UseTransType = kkapp.TransTypeShard
	GateTCPAddr  = "127.0.0.1:19090"
	GateWSAddr   = "127.0.0.1:19091"
	RpcAddr      = "127.0.0.1:19092"
)

var (
	ClientConnNum      = 2000
	ClientConnDelay    = 5 * time.Millisecond
	ClientSendInterval = 200 * time.Millisecond
)

// WithGate 仅 aserver：是否同时启动 gate 组件
var WithGate = false
