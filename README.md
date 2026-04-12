# kkdg

Go 语言实现的游戏/分布式服务端引擎，提供网络层、集群通信、应用框架等基础能力。

## 模块结构

kkdg/
├── kkapp/           # 应用框架（ProtoActor：Application + Component + kkactor）
│   ├── component/   # Application、Component 生命周期
│   ├── kkactor/     # Actor 寻址、远程传输（atransnats、atransrelay、registry/hubtcp 等）
│   ├── transport/   # Gate↔Logic 转发（nats/rpc/shard）
│   └── msgreceiver/ # 消息分发
├── kknet/           # 网络层
│   ├── kktcp/       # TCP（gnet）
│   ├── kktcptls/    # TCP TLS
│   ├── kkws/        # WebSocket（gorilla）
│   ├── kkgws/       # WebSocket（lxzan/gws）
│   ├── kkprocessor/ # 读/写处理器
│   └── kkpacket/    # 封包、MsgRouter
├── remotes/         # 远程能力
│   ├── kkrpc/       # RPC（请求响应/单向/异步）
│   ├── kkcluster/   # NATS 集群
│   └── kkdiscovery/ # 服务发现
├── proto/           # 协议（FlatBuffers、Protobuf）
├── utils/           # 工具（buffer、codec、queue、timingwheel 等）
├── kkmetrics/       # Prometheus/OpenTelemetry
└── tools/           # 代码生成（fbspb、fbs2struct），消息ID生成(msdid)

## 架构分层

三、架构分层
层级 | 职责 | 实现
业务层 | 业务逻辑、消息处理 | ccgate、ccgame、MsgReceiver
应用层（Actor） | 节点、组件、生命周期 | kkapp：Application、Component
应用层（非 Actor） | 模块树、模块生命周期 | kkmodule：IModule、Module
Actor层 | 透明寻址、远程路由 | kkactor：ActorFramework、LocalActorManager
传输层 | Gate↔Logic 转发 | gatetrans、gametrans
远程层 | RPC、集群、发现 | kkrpc、kkcluster、kkdiscovery
网络层 | 连接、封包、处理 | kknet
基础设施 | 存储、协议、工具 | proto、utils

## 其他非框架目录 [other]

zothers/internal/demo1 游戏应用完整示例。可以基于此模版，实现自己的游戏应用业务
zothers/internal/examples 示例
zothers/internal/test 白盒测试

## 架构概览

- **连接层**：连接管理、心跳、断线重连
- **处理器层**：编解码、粘包拆包、消息分发
- **业务层**：监听消息并处理业务逻辑

典型部署：**Gate（网关）** 接收客户端连接，通过 **shard/rpc/nats** 与 **Logic（业务服）** 互通。

## 快速开始

框架本身作为库使用，并不提供完整应用，但是有提供完整的快速构建应用的设施。
示例详见 zothers/internal/demo1。

1. kkapp/kkappm 应用层。
kkapp提供了actor模式的应用层框架；kkappm提供了模块树形式的应用层框架；
具体选型根据自己喜好，也可以抛开这两种方式，实现自己的应用层模型。

2. kkapp的核心理念是透明actor，分布式部署简单，扩容方便。
节点本身也是actor，可以做到不止应用内部的业务actor透明，节点本身也可以被透明寻址。
业务服与网关之间可以通过transport传输层进行通信，高性能；
也可以将业务服和网关视为actor，利用actorFramework进行通信，统一，但是性能不如transport，
后续可以通过仿tansport（kkapp/transport）优化actor传输层（kkactor/transport）

3. 本库的核心在于提供分布式框架基础。
核心1：kknet，基础网络设施，包含tcp/ws
核心2：kkpacket，封包解包
核心3：remotes，分布式基础，包含集群、服务发现、rpc、分布式事件总线
核心4：tools，各种基础设施，编解码、时间轮、队列、pool等

### 依赖

- Go 1.25.3
- NATS（集群与服务发现）

### 运行示例

```bash
# 集群示例（需先启动 NATS）
cd zothers/examples/examcluster && go run main.go

# 服务发现示例（需先启动 NATS）
cd zothers/examples/examdiscovery && go run main.go

# WebSocket Echo 示例
cd zothers/examples/examws && go run main.go

# TCP Echo 示例
cd zothers/examples/examtcp && go run main.go

# TCP TLS Echo 示例（使用自签名证书）
cd zothers/examples/examtcptls && go run main.go

# RPC 示例（请求响应 + 单向 + 异步）
cd zothers/examples/examrpc && go run main.go
```

### 测试

```bash
go test ./...
go test -bench=. -benchmem ./...
```

## 核心能力

- **网络**：TCP / WebSocket / TLS，支持多 Codec（JSON、ProtoBuf、MsgPack、FlatBuffer）
- **集群**：基于 NATS 的 Publish/Request，支持服务发现与节点类型订阅
- **存储**：MySQL（kkdb + GORM 引擎与 CRUD）、Redis（kkredis 单机引擎与 Get/Set 等封装）
- **队列**：BBQueue（环形数组）、NNQueue（链表，内存更省）
- **应用（Actor 路线）**：`kkapp` — `Application` + `Component` + `kkactor`
- **应用（非 Actor 路线）**：`kkmodule` — 模块树与 `OnInit` / `OnStop`

## 运行时全局变量清单

见各模块ConfigDefaults接口
