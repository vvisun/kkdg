package kkwstls

/**

基于TLS/WSS的websocket实现，配合gobwas/ws完成握手与帧处理。

当前实现方式：
1. 目前的实现是每个连接一个读携程和一个写携程
2. 写携程在写队列满时 Send 会阻塞，直到有空位或连接关闭；不会丢包也不会返回“队列满”错误

后续优化计划：
1. 读携程依旧保持每个连接一个读携程。
2. 合并多个连接的写携程，减少资源消耗。
 合并写携程后，写携程需要支持多连接并发写，并保证写顺序。
 维护一个发送队列，每个连接一个发送队列，发送队列需要支持多连接并发写，并保证写顺序。


gobwas/ws: 是一个高性能的websocket库。
地址: https://github.com/gobwas/ws

----------------------------------------------------------------------------------------------

WSS启动示例:

	cert, _ := tls.LoadX509KeyPair("server.crt", "server.key")
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	svr := kkwstls.NewServer(":8443", handler, kknet.WithTLSConfig(tlsCfg))
	svr.SetPath("/ws")
	_ = svr.Start()

WSS客户端示例:

	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	cli := kkwstls.NewClient("wss://127.0.0.1:8443/ws", handler, kknet.WithTLSConfig(tlsCfg))
	_ = cli.Connect()

WSS客户端(校验证书):

	caPem, _ := os.ReadFile("ca.crt")
	pool := x509.NewCertPool()
	_ = pool.AppendCertsFromPEM(caPem)
	tlsCfg := &tls.Config{RootCAs: pool}
	cli := kkwstls.NewClient("wss://localhost:8443/ws", handler, kknet.WithTLSConfig(tlsCfg))
	_ = cli.Connect()

自签证书生成(OpenSSL):

	openssl req -x509 -newkey rsa:2048 -nodes -keyout server.key -out server.crt -days 365 -subj "/CN=localhost"

自签CA并签发服务器证书(OpenSSL):

	# 1) 生成CA证书
	openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 3650 -subj "/CN=kkwstls-ca"

	# 2) 生成服务器私钥与CSR
	openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=localhost"

	# 3) 使用CA签发服务器证书
	openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365

*/
