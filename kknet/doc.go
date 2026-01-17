package kknet

/*
kknet是一个网络库，用于实现网络通信。
gnet + ants + protoactor-go

bytes或队列，可以利用utils/buffers 或 utils/queues 或 github.com/panjf2000/gnet/v2/pkg下的buffer|queue。

gnet: 是一个高性能的网络库，支持TCP和UDP协议。
地址: https://github.com/panjf2000/gnet
ants: 是一个高性能的goroutine池。
地址: https://github.com/panjf2000/ants
protoactor-go: 是一个基于Actor模型的并发编程框架。
地址: https://github.com/asynkron/protoactor-go

支持 TCP/WS 的 TLS/SSL（通过 Options.TLSConfig 配置）。
*/
