# ttgws：gws 压测（对比 ttws）

与 `ttws` 同场景的 WebSocket 压测，用于对比 **kkws**（gorilla/websocket）与 **gws**（kknet/engines/gws）的表现。

## 对比

| 项目   | ttws                | ttgws           |
|--------|---------------------|-----------------|
| 服务端 | `other/tests/ttws/ttwsserver`  | `other/tests/ttgws/ttgwsserver`  |
| 客户端 | `other/tests/ttws/ttwsclient`  | `other/tests/ttgws/ttgwsclient`  |
| 库     | kknet/kkws (gorilla) | kknet/engines/gws       |
| 端口   | 8080，path `/ws`    | 8080，path `/ws` |
| 参数   | -addr -conn -size -interval | 同左 |

## 运行

**服务端（先起一个）：**
```bash
go run ./other/tests/ttgws/ttgwsserver
# 或
run_ttgws_server.bat
```

**客户端（与 ttws 相同参数）：**
```bash
go run ./other/tests/ttgws/ttgwsclient -addr=localhost:8080 -conn=1000 -size=64 -interval=10ms
# 或
run_ttgws_client.bat
```

## 对比方式

1. 起 **ttws** 服务端 + ttws 客户端，看日志里的连接数、收包数。
2. 停掉后起 **ttgws** 服务端 + ttgws 客户端，用相同 `-conn -size -interval` 再跑一轮。
3. 对比两边在相同连接数、消息大小、发送间隔下的 CPU/内存与收包速率。

注意：ttws 客户端用 kkpacket 打流式包；ttgws 客户端发原始二进制 WebSocket 帧，消息大小一致即可对比。

## Windows 报错 "buffer space or queue was full"

系统 TCP 缓冲区或端口队列被占满时会出现。处理方式：

1. **减小并发**：`-conn=500` 或更小。
2. **拉长建连间隔**：`-connDelay=10ms` 或 `20ms`（ttgws/ttws 客户端均支持 `-connDelay`）。
3. **示例**：`go run ./other/tests/ttgws/ttgwsclient -addr=localhost:8080 -conn=500 -connDelay=10ms`
