package examapp

import (
	"flag"
	"os"
	"time"

	"github.com/vvisun/kkdg/kkapp/transport"
)

// 默认值；通过 ParseFlags() 可由命令行参数覆盖
var (
	NatsURL      = "nats://127.0.0.1:4222"
	UseTransType = transport.TransTypeShard
	TransAddr    = "127.0.0.1:19092"

	// GateTCPAddr 仅 agate：网关服 TCP 监听地址
	GateTCPAddr = "127.0.0.1:19090"
	// GateWSAddr 仅 agate：网关服 WebSocket 监听地址
	GateWSAddr = "127.0.0.1:19091"

	// LogicNodeID 仅 aserver: 逻辑服节点ID，便于多逻辑服测试
	LogicNodeID = "game1"
)

var (
	ClientConnNum      = 2000
	ClientConnDelay    = 5 * time.Millisecond
	ClientSendInterval = 200 * time.Millisecond
)

// ParseFlags 解析命令行参数并更新 NatsURL、GateTCPAddr 等包变量。
// 应在各 main 中在其它逻辑前调用（可传 flag.Args() 或 nil 使用 os.Args[1:]）。
func ParseFlags(args []string) {
	fs := flag.NewFlagSet("examapp", flag.ExitOnError)
	fs.StringVar(&NatsURL, "nats-url", NatsURL, "NATS 地址，如 nats://127.0.0.1:4222")
	fs.StringVar(&GateTCPAddr, "gate-tcp", GateTCPAddr, "Gate TCP 监听地址")
	fs.StringVar(&GateWSAddr, "gate-ws", GateWSAddr, "Gate WebSocket 监听地址")
	fs.StringVar(&TransAddr, "trans-addr", TransAddr, "转发地址")
	fs.StringVar(&UseTransType, "trans", UseTransType, "转发类型: nats | rpc | shard")
	fs.StringVar(&LogicNodeID, "node-id", LogicNodeID, "逻辑服节点ID（仅 aserver）")

	fs.IntVar(&ClientConnNum, "conn-num", ClientConnNum, "客户端连接数（仅 aclient）")
	fs.DurationVar(&ClientConnDelay, "conn-delay", ClientConnDelay, "客户端建连间隔（仅 aclient）")
	fs.DurationVar(&ClientSendInterval, "send-interval", ClientSendInterval, "客户端发送间隔（仅 aclient）")

	if args == nil {
		_ = fs.Parse(os.Args[1:])
	} else {
		_ = fs.Parse(args)
	}

	// 归一化 trans
	if UseTransType != transport.TransTypeNats && UseTransType != transport.TransTypeRpc && UseTransType != transport.TransTypeShard {
		UseTransType = transport.TransTypeNats
	}
}
