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

WSS客户端示例:

	tlsCfg := &tls.Config{InsecureSkipVerify: true}
	cli := kkwsgob.NewClient("wss://127.0.0.1:8443/ws", handler, kknet.WithTLSConfig(tlsCfg))
	_ = cli.Connect()

WSS客户端(校验证书):

	caPem, _ := os.ReadFile("ca.crt")
	pool := x509.NewCertPool()
	_ = pool.AppendCertsFromPEM(caPem)
	tlsCfg := &tls.Config{RootCAs: pool}
	cli := kkwsgob.NewClient("wss://localhost:8443/ws", handler, kknet.WithTLSConfig(tlsCfg))
	_ = cli.Connect()

自签证书生成(OpenSSL):

	openssl req -x509 -newkey rsa:2048 -nodes -keyout server.key -out server.crt -days 365 -subj "/CN=localhost"

自签CA并签发服务器证书(OpenSSL):

	# 1) 生成CA证书
	openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days 3650 -subj "/CN=kkwsgob-ca"

	# 2) 生成服务器私钥与CSR
	openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=localhost"

	# 3) 使用CA签发服务器证书
	openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365

*/
