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

下面这份清单用于后续逐条评估“哪些需要调整，哪些需要保持”。

统计范围：

- 收录运行时相关的全局可变状态、默认单例、全局计数器
- 不收录 `const`
- 不收录错误变量（如 `ErrXXX`）
- 不收录接口断言（如 `var _ IFoo = (*Bar)(nil)`）
- 不收录对象池及其配套复位模板
- 不收录测试代码、示例代码、生成代码中的全局变量

建议后续标注时可使用：

- `必须保持`
- `默认实现且可配置`
- `必须移除`
- `待调整`
- `待确认`

以下按“后续真正要动代码的优先级”排序：越靠前，越值得先讨论或先改。

### 默认实现且可配置

| 路径 | 全局变量 | 作用 | 备注 |
| ------ | ---------- | ------ | ---------- |
| `utils/xnet/resolver.go` | `globalPublicIPResolver` | 全局公网 IP 解析器 | 提供默认实现，可配置 |
| `utils/xnet/resolver.go` | `globalPrivateIPResolver` | 全局私网 IP 解析器 | 提供默认实现，可配置 |
| `utils/xnet/resolver.go` | `urls` | 默认公网 IP 查询地址列表 | 提供默认实现，可配置 |
| `kkapp/component/app.go` | `globalActorFramework` | 默认全局 `ActorFramework` 单例 | 提供默认全局，不使用全局时传参即可 |
| `kkapp/kkactor/actorremotes/registry.go` | `defaultMessageRegistry` | 远程 Actor 消息类型全局注册表 | 同 `gMsgPacket`，作为默认注册表存在 |
| `kkapp/kkactor/actorremotes/registry.go` | `msgCodec` | 远程 Actor 协议默认编解码器 | 同 `gMsgPacket`，提供默认实现并允许切换 |
| `kkapp/packet.go` | `gTransMsgPacket` | 网关与业务服之间的默认消息封包/路由器 | 同 `gMsgPacket` |
| `kkapp/packet.go` | `gMsgPacket` | 网关与客户端之间的默认消息封包/路由器 | 初始化阶段设置即可，协议格式通常较早确定，后续改动不频繁 |
| `kknet/kkpacket/defaults.go` | `gMaxPacketSize` | 全局默认最大包长 | 同 `gMsgPacket`，提供默认实现并允许设置 |
| `kknet/kkpacket/defaults.go` | `defaultStreamPacket` | 全局默认流式拆包器 | 同 `gMsgPacket`，提供默认实现并允许替换 |
| `kknet/kkpacket/defaults.go` | `gByteOrder` | 全局字节序 | 同 `gMsgPacket`，提供默认实现并允许设置 |
| `remotes/kkdiscovery/dnats/nats_option.go` | `msgCodec` | NATS 服务发现默认编解码器 | 同 `gMsgPacket` |
| `utils/kklog/logger.go` | `defaultLogger` | 默认控制台日志实例 | 提供默认实现并可配置 |
| `utils/kklog/logger.go` | `loggers` | 按名称缓存的日志实例表 | 提供默认实现并可配置 |
| `utils/kklog/logger.go` | `nodeID` | 日志上下文中的全局节点 ID | 提供默认实现并可配置 |
| `utils/kklog/logger.go` | `printLevel` | 全局日志打印级别 | 提供默认实现并可配置 |
| `utils/kklog/logger.go` | `fileNameVarMap` | 日志文件名模板变量表 | 提供默认实现并可配置 |
| `utils/kklog/logger.go` | `DateTimeFormat` | 全局默认日志时间格式 | 提供默认实现并可配置 |
| `utils/xrand/rand.go` | `globalRand` | 全局随机数发生器 | 默认实现可用，局部场景也可自建随机源 |

### 必须保持

| 路径 | 全局变量 | 作用 | 备注 |
| ------ | ---------- | ------ | ---------- |
| `remotes/kkrpc/regist.go` | `gRpcManager` | RPC 方法/类型全局注册管理器 | 否则容易协议混乱 |
| `kkapp/comps/ccgate/logic_total.go` | `gLogicTotalMgr` | 网关视角的逻辑节点绑定会话统计 | 统计的就是本节点总量，非全局反而不准确 |
| `remotes/kkrpc/facade.go` | `req_id` | RPC 请求 ID 全局递增计数器 | 否则无法区分不同请求 |
| `remotes/kkcluster/req_id.go` | `gRequestIDSeq` | 集群请求 ID 全局递增计数器 | 否则无法区分不同请求 |
| `kknet/id.go` | `connIDCounter` | 连接 ID 全局递增计数器 | 否则无法区分不同连接 |
| `remotes/kkcluster/cnats/msg_pool.go` | `msgCodec` | NATS 集群消息默认编解码器 | 当前作为集群协议统一编解码入口保留 |

### 必须移除

当前标注中暂无此类项，后续如你补充，我再继续收进清单。

补充说明：

- `gLogicTotalMgr` 统计的是“网关视角下仍绑定在逻辑节点上的会话”，不是纯 TCP 在线数；
- 这类统计不会因网关侧断连立刻回落，是否解绑依赖上层玩家管理确认玩家已从原逻辑服安全移除；
- `globalActorFramework`、`gMsgPacket`、`gTransMsgPacket`、`gRpcManager`、`globalEventBus` 等都属于典型“进程级默认单例”；
- `req_id`、`gRequestIDSeq`、`connIDCounter` 等属于“全局递增计数器”。

## License

见各子模块声明。
