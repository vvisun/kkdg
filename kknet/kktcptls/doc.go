// Package kktcptls 基于标准库 net + crypto/tls 实现 TCP 客户端与服务器，支持 TLS；TLSConfig 为 nil 时可退化为纯 TCP。
//
// 与 kktcp（gnet）的区别：kktcp 基于 gnet 事件循环，高性能但不支持 TLS；
// kktcptls 基于 net.Conn，每连接两个 goroutine（读+写），原生支持 crypto/tls，适用于需 TLS 或连接数不大的场景。
package kktcptls
