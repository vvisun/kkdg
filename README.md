# kkdg

Go 语言实现的游戏/分布式服务端引擎，提供网络层、集群通信、应用框架等基础能力。

## 模块结构

kkdg/
├── kkapp/           # 应用框架（ProtoActor + Component）
│   ├── component/   # Application、Component 生命周期
│   ├── comps/       # ccgate（网关）、ccgame（业务服）
│   ├── kkactor/     # Actor 寻址、远程传输（actornats/actorrpc/actorshard）
│   ├── kkmodule/    # 模块树
│   └── transport/   # Gate↔Logic 转发（nats/rpc/shard）
├── kknet/           # 网络层
│   ├── kktcp/       # TCP（gnet）
│   ├── kktcptls/    # TCP TLS
│   ├── kkws/        # WebSocket（gorilla）
│   ├── kkgws/       # WebSocket（lxzan/gws）
│   ├── kkprocessor/ # 读/写处理器
│   ├── kkpacket/    # 封包、MsgRouter
│   └── msgreceiver/ # 消息分发
├── remotes/         # 远程能力
│   ├── kkrpc/       # RPC（请求响应/单向/异步）
│   ├── kkcluster/   # NATS 集群
│   └── kkdiscovery/ # 服务发现
├── storage/         # 存储
│   ├── kkdb/        # MySQL/GORM
│   └── kkredis/     # Redis
├── proto/           # 协议（FlatBuffers、Protobuf）
├── utils/           # 工具（buffer、codec、queue、timingwheel 等）
├── kkmetrics/       # Prometheus/OpenTelemetry
└── tools/           # 代码生成（fbspb、fbs2struct），消息ID生成(msdid)

## 架构分层

三、架构分层
层级 | 职责 | 实现
业务层 | 业务逻辑、消息处理 | ccgate、ccgame、MsgReceiver
应用层 | 节点、组件、生命周期 | Application、Component
Actor层 | 透明寻址、远程路由 | ActorFramework、ActorLocator
传输层 | Gate↔Logic 转发 | gatetrans、gametrans
远程层 | RPC、集群、发现 | kkrpc、kkcluster、kkdiscovery
网络层 | 连接、封包、处理 | kknet
基础设施 | 存储、协议、工具 | storage、proto、utils

## 其他非框架目录 [other]

other/examples 示例
other/test 白盒测试

## 架构概览

- **连接层**：连接管理、心跳、断线重连
- **处理器层**：编解码、粘包拆包、消息分发
- **业务层**：监听消息并处理业务逻辑

典型部署：**Gate（网关）** 接收客户端连接，通过 **shard/rpc/nats** 与 **Logic（业务服）** 互通。

## 快速开始

### 依赖

- Go 1.25.3
- NATS（集群与服务发现）

### 运行示例

```bash
# 集群示例（需先启动 NATS）
cd other/examples/examcluster && go run main.go

# 服务发现示例（需先启动 NATS）
cd other/examples/examdiscovery && go run main.go

# WebSocket Echo 示例
cd other/examples/examws && go run main.go

# TCP Echo 示例
cd other/examples/examtcp && go run main.go

# TCP TLS Echo 示例（使用自签名证书）
cd other/examples/examtcptls && go run main.go

# RPC 示例（请求响应 + 单向 + 异步）
cd other/examples/examrpc && go run main.go
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
- **应用**：`Application` + `Component` 生命周期管理

## 运行时全局变量清单

见各模块ConfigDefaults接口
