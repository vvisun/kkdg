package kkwsgob

/**

采用gnet + gobwas/ws组合，实现百万级websocket。

gnet: 是一个高性能的网络库，支持TCP和UDP协议。
地址: https://github.com/panjf2000/gnet
gobwas/ws: 是一个高性能的websocket库。
地址: https://github.com/gobwas/ws

TLS/WSS: 通过标准TLS监听与gobwas/ws握手支持，保持kknet接口一致。

WSS启动示例:

	cert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	svr := kkwsgob.NewServer(":8443", handler, kknet.WithTLSConfig(tlsCfg))
	svr.SetPath("/ws")
	_ = svr.Start()

*/
