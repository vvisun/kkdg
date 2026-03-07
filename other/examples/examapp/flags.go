package examapp

import (
	"flag"
	"os"

	"github.com/vvisun/kkdg/kkapp"
)

// ParseFlags 解析命令行参数并更新 NatsURL、GateTCPAddr 等包变量。
// 应在各 main 中在其它逻辑前调用（可传 flag.Args() 或 nil 使用 os.Args[1:]）。
func ParseFlags(args []string) {
	fs := flag.NewFlagSet("examapp", flag.ExitOnError)
	fs.StringVar(&NatsURL, "nats-url", NatsURL, "NATS 地址，如 nats://127.0.0.1:4222")
	fs.StringVar(&GateTCPAddr, "gate-tcp", GateTCPAddr, "Gate TCP 监听地址")
	fs.StringVar(&GateWSAddr, "gate-ws", GateWSAddr, "Gate WebSocket 监听地址")
	fs.StringVar(&RpcAddr, "rpc-addr", RpcAddr, "RPC 地址")
	fs.StringVar(&UseTransType, "trans", UseTransType, "转发类型: nats | rpc")

	fs.IntVar(&ClientConnNum, "conn-num", ClientConnNum, "客户端连接数（仅 aclient）")
	fs.DurationVar(&ClientConnDelay, "conn-delay", ClientConnDelay, "客户端建连间隔（仅 aclient）")
	fs.DurationVar(&ClientSendInterval, "send-interval", ClientSendInterval, "客户端发送间隔（仅 aclient）")
	fs.BoolVar(&WithGate, "with-gate", WithGate, "aserver 是否同时启动 gate")

	if args == nil {
		_ = fs.Parse(os.Args[1:])
	} else {
		_ = fs.Parse(args)
	}

	// 归一化 trans
	if UseTransType != kkapp.TransTypeNats && UseTransType != kkapp.TransTypeRpc {
		UseTransType = kkapp.TransTypeNats
	}
}
