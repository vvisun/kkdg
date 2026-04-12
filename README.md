# kkdg

Go 语言实现的游戏 / 分布式服务端引擎：**网络层、远程通信（集群 / RPC / 发现 / 事件总线）、应用框架（Actor / 模块树）** 等基础能力以库形式提供。

---

## 模块结构

```
kkdg/
├── kkapp/            # 应用框架（ProtoActor：Application + Component + kkactor）
│   ├── component/    # Application、Component 生命周期
│   ├── kkactor/      # Actor 寻址、远程传输（atransnats、atransrelay、registry/hubtcp 等）
│   ├── transport/    # Gate ↔ Logic 转发（nats / rpc / shard）
│   └── msgreceiver/  # 消息分发
├── kkappm/           # 非 Actor 应用层：模块树（IModule、Module）
├── kknet/            # 网络层
│   ├── kktcp/        # TCP（gnet）
│   ├── kktcptls/     # TCP TLS
│   ├── kkws/         # WebSocket（gorilla）
│   ├── kkgws/        # WebSocket（lxzan/gws）
│   ├── kkprocessor/  # 读 / 写处理器
│   └── kkpacket/     # 封包、MsgRouter
├── remotes/          # 远程能力
│   ├── kkrpc/        # RPC（请求 / 响应、单向、异步）
│   ├── kkcluster/    # 基于 NATS 的集群消息
│   ├── kkdiscovery/  # 服务发现
│   └── kkeventbus/   # 发布 / 订阅事件总线（buslocal / busnats）
├── proto/            # 协议（FlatBuffers、Protobuf 等）
├── utils/            # 工具（buffer、codec、queue、timingwheel 等）
├── kkmetrics/        # Prometheus / OpenTelemetry
└── tools/            # 代码生成（fbspb、fbs2struct）、消息 ID 等
```

---

## 架构分层

| 层级 | 职责 | 主要实现 |
|------|------|----------|
| 业务层 | 业务逻辑、消息处理 | 示例：`ccgate`、`ccgame`、MsgReceiver |
| 应用层（Actor） | 节点、组件、生命周期 | `kkapp`：Application、Component |
| 应用层（非 Actor） | 模块树、模块生命周期 | `kkappm`：IModule、Module |
| Actor 层 | 透明寻址、远程路由 | `kkactor`：ActorFramework、LocalActorManager |
| 传输层 | Gate ↔ Logic 转发 | `gatetrans`、`gametrans` |
| 远程层 | RPC、集群、发现、事件总线 | `kkrpc`、`kkcluster`、`kkdiscovery`、`kkeventbus` |
| 网络层 | 连接、封包、处理 | `kknet` |
| 基础设施 | 协议、工具、指标 | `proto`、`utils`、`kkmetrics` |

**典型部署**：**Gate（网关）**接客户端连接，经 **shard / rpc / nats** 与 **Logic（业务服）** 互通。

**运行时处理管线（概念）**：

- **连接层**：连接管理、心跳、断线重连  
- **处理器层**：编解码、粘包拆包、消息分发  
- **业务层**：订阅并处理业务消息  

---

## 仓库内非「框架内核」目录

| 路径 | 说明 |
|------|------|
| `zothers/internal/demo1` | 较完整的游戏向示例，可在此基础上做自己的业务 |
| `zothers/internal/examples` | 小粒度示例（集群、发现、网络 echo、RPC 等） |
| `zothers/internal/test` | 白盒 / 集成类测试 |

---

## 快速开始

本仓库**以库为主**，不单独提供一套可运行的「官方完整游戏」，但具备快速搭应用的配套与示例。

1. **应用层选型**  
   - **`kkapp`**：Actor 模式（Application + Component + `kkactor`）。  
   - **`kkappm`**：模块树（`OnInit` / `OnStop` 等）。  
   也可不用二者，自建应用层模型。

2. **Actor 与传输**  
   `kkapp` 强调**透明 Actor**，便于分布式与扩容；节点本身也可作为 Actor 被寻址。  
   网关与业务服之间推荐 **`transport`** 转发（偏性能）；若全流程走 **ActorFramework**（`kkactor/transport`），模型更统一但通常更慢，后续可朝 `kkapp/transport` 的思路优化 Actor 传输层。

3. **可复用的「地基」**  
   - **kknet**：TCP / WebSocket 等基础网络  
   - **kkpacket**：封包与路由  
   - **remotes**：集群、发现、RPC、事件总线（**`kkeventbus` 用法见包内 `facade.go` 注释**，推荐通过 `kkapp` 的 `SetExtData` / `GetExtData` 挂实例）  
   - **tools / utils**：编解码、时间轮、队列、对象池等  

### 依赖

- **Go**：见 `go.mod`（当前为 Go 1.25.3）  
- **NATS**：集群、服务发现及多数 `remotes` 示例依赖本机或可访问的 NATS  

---

## 运行示例

在**仓库根目录**（`go.mod` 所在目录）执行。依赖 NATS 的示例需先启动 NATS（默认常为 `nats://127.0.0.1:4222`，以各示例为准）。

```bash
# 集群（需 NATS）
cd zothers/internal/examples/examcluster && go run .

# 服务发现（需 NATS）
cd zothers/internal/examples/examdiscovery && go run .

# WebSocket Echo
cd zothers/internal/examples/examws && go run .

# TCP Echo
cd zothers/internal/examples/examtcp && go run .

# TCP TLS Echo（示例内自签名证书等）
cd zothers/internal/examples/examtcptls && go run .

# RPC（请求 / 响应 + 单向 + 异步）
cd zothers/internal/examples/examrpc && go run .
```

更完整的业务拼装可参考 **`zothers/internal/demo1`**。

---

## 测试

```bash
go test ./...
go test -bench=. -benchmem ./...
```

部分测试依赖本机 NATS 或环境变量；不可用时常以 **Skip** 处理，具体见各包 `*_test.go`。

---

## 核心能力摘要

- **网络**：TCP / WebSocket / TLS；多种 Codec（JSON、Protobuf、MsgPack、FlatBuffers 等）  
- **集群**：基于 NATS 的发布 / 请求响应；可选服务发现、按节点类型订阅  
- **事件总线**：进程内（`buslocal`）或 NATS（`busnats`）  
- **队列**：BBQueue（环形）、NNQueue（链表）等（见 `utils`）  
- **应用**：Actor 路线（`kkapp`）或模块树（`kkappm`）  
- **观测**：`kkmetrics`（Prometheus / OpenTelemetry）  

存储（MySQL、Redis 等）通常按业务在应用侧接入；若仓库内另有存储封装包，以其 README 为准。

---

## 配置与全局默认值

各模块中与运行相关的 **默认值、可配置项**，以各包内说明及 **`*ConfigDefaults*` / 配置结构体** 为准（README 不逐项复制，避免与代码脱节）。
