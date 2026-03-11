# kkdg

Go 语言实现的游戏/分布式服务端引擎，提供网络层、集群通信、应用框架等基础能力。

## 模块结构

| 模块 | 路径 | 说明 |
| ------ | ------ | ------ |
| **kkapp** | `kkapp/` | 应用框架：基于 ProtoActor 的组件化节点，支持 Gate、Game 等业务组件 |
| **kknet** | `kknet/` | 网络层：TCP / TCP TLS / WebSocket 服务端与客户端。未来可考虑加入UDP、KCP |
| **kkprocessor** | `kknet/kkprocessor/` | 消息处理器：读/写队列、粘包拆包、批量发送 |
| **kkpacket** | `kknet/kkpacket/` | 封包协议：流式封包、消息路由 |
| **remotes** | `remotes/` | 远程能力：RPC、集群（NATS）、服务发现 |
| **storage** | `storage/` | 存储：kkdb（MySQL/GORM 配置与 CRUD）、kkredis（Redis 单机客户端与常用命令封装） |
| **proto** | `proto/` | 协议定义：FlatBuffers、Protobuf |
| **utils** | `utils/` | 工具库：buffer、codec、时间轮、队列、转换等 |

## 架构概览

- **连接层**：连接管理、心跳、断线重连
- **处理器层**：编解码、粘包拆包、消息分发
- **业务层**：监听消息并处理业务逻辑

典型部署：**Gate（网关）** 接收客户端连接，通过 **NATS** 与 **Logic（业务服）** 互通。

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
