package kkwstls

/**

基于TLS/WSS的websocket实现，配合gobwas/ws完成握手与帧处理。

gobwas/ws: https://github.com/gobwas/ws

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
