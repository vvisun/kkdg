package kkwsgob

/**

采用gnet + gobwas/ws组合，实现百万级websocket。

gnet: 是一个高性能的网络库，支持TCP和UDP协议。
地址: https://github.com/panjf2000/gnet
gobwas/ws: 是一个高性能的websocket库。
地址: https://github.com/gobwas/ws

TLS/WSS: 请使用 kkwstls 包。

todo: 当前实现存在丢包风险。详见AsyncWrite调用处

*/
