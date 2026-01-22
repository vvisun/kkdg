package testtls

// func TestKKTcpTLS(t *testing.T) {
// 	serverTLS, clientTLS := tlsConfigPair(t)
// 	addr := freeTCPAddr(t)

// 	serverHandler := &testHandler{
// 		onMessage: func(c kknet.IConn, data []byte) {
// 			_ = c.Send(data)
// 		},
// 	}
// 	server := kktcp.NewServer(addr, serverHandler, kknet.WithTLSConfig(serverTLS))
// 	if err := server.Start(); err != nil {
// 		t.Fatalf("server start: %v", err)
// 	}
// 	defer func() { _ = server.Stop() }()

// 	replyCh := make(chan []byte, 1)
// 	clientHandler := &testHandler{
// 		onMessage: func(c kknet.IConn, data []byte) {
// 			replyCh <- data
// 		},
// 	}
// 	client := kktcp.NewClient(addr, clientHandler, kknet.WithTLSConfig(clientTLS))
// 	if err := client.Connect(); err != nil {
// 		t.Fatalf("client connect: %v", err)
// 	}
// 	defer func() { _ = client.Close() }()

// 	payload := []byte("tls")
// 	if err := client.Send(payload); err != nil {
// 		t.Fatalf("client send: %v", err)
// 	}

// 	select {
// 	case got := <-replyCh:
// 		if string(got) != string(payload) {
// 			t.Fatalf("unexpected reply: %s", got)
// 		}
// 	case <-time.After(2 * time.Second):
// 		t.Fatal("tls reply timeout")
// 	}
// }
