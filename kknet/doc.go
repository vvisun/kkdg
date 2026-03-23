// Package kknet 提供网络框架，分层设计如下：
//
//  1. 连接层：负责连接管理、心跳、断线重连（tcp/ws/...）
//  2. 消息处理器层：网络数据处理，负责消息的编码解码、粘包拆包、分发到业务逻辑层。
//     消息处理器应在连接建立时开始消费队列中的消息；连接关闭时，不再接受新消息入队，但是继续消费完队列中的消息后再关闭。
//  3. 业务逻辑层：监听分发出来的消息，并处理业务逻辑
//
// 子包说明：
//   - kkgws: 基于“github.com/lxzan/gws”实现的websocket服务器和客户端。支持TLS
//   - kkws: 基于“github.com/gorilla/websocket”实现的websocket服务器和客户端。支持TLS
//   - kktcp: 基于“github.com/panjf2000/gnet”实现的tcp服务器和客户端。不支持TLS
//   - kktcptls: 基于net.Conn实现的tcp服务器和客户端。支持TLS
//   - kkprocessor: 消息处理器，提供消息的编码解码、粘包拆包、分发到业务逻辑层。
//   - kkpacket: 封包拆包工具，编解码。
package kknet
