package kkws

import (
	"testing"
)

func BenchmarkWSConn_SendBuffer(b *testing.B) {
	wsUrl := "ws://localhost:8080/ws"

	wsServer := NewServer(wsUrl, nil, nil)
	wsServer.Start()
	defer wsServer.Stop()

	wsClient := NewClient(wsUrl, nil, nil)
	wsClient.Connect()
	defer wsClient.Close()
}
