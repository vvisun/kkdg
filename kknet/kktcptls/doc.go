package kktcptls

/*
基于标准库 net + crypto/tls 实现的 TCP 客户端和服务器。
支持 TLS 加密连接，也可在 TLSConfig 为 nil 时退化为纯 TCP。

与 kktcp (gnet) 的区别：
  - kktcp 基于 gnet 事件循环，高性能但不支持 TLS（客户端）。
  - kktcptls 基于标准库 net.Conn，每连接两个 goroutine（读+写），
    原生支持 crypto/tls。适用于需要 TLS 的场景或连接数不大的内部服务。
*/
