// 集成测试：启动网关 + 逻辑服，WS 客户端完成一次上下行往返。
// 需要占用端口 9981（TCP）、8080（WS），且 -short 时跳过。
// 运行：go test -run Integration -v ./kkapp/framework/extgate/...
package extgate

import (
	"encoding/json"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vvisun/kkdg/kkapp/framework/extlogic"
	"github.com/vvisun/kkdg/kkapp/framework/extmsg"
)

func waitTCP(addr string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			_ = c.Close()
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func TestIntegration_GatewayLogicRoundtrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in -short mode")
	}

	// 1) 启动网关（阻塞在 ListenAndServe，放 goroutine）
	go StartUp()
	if !waitTCP("127.0.0.1:"+GatewayTCPPort, 5*time.Second) {
		t.Skip("gateway TCP not ready in time (is 9981 free?)")
	}
	if !waitTCP("127.0.0.1:"+GatewayWSPort, 3*time.Second) {
		t.Skip("gateway WS not ready in time (is 8080 free?)")
	}

	// 2) 启动逻辑服（阻塞在 select{}，放 goroutine）
	go extlogic.StartUp()
	time.Sleep(3 * time.Second) // 等待 8 条连接 + 注册

	// 3) WS 客户端连接
	wsURL := "ws://127.0.0.1:" + GatewayWSPort + "/ws"
	dl := websocket.Dialer{HandshakeTimeout: 5 * time.Second}
	conn, _, err := dl.Dial(wsURL, http.Header{})
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	// 4) 心跳
	heartbeat, _ := json.Marshal(map[string]any{"cmd": extmsg.CmdHeartbeat, "uid": uint64(10001)})
	if err := conn.WriteMessage(websocket.TextMessage, heartbeat); err != nil {
		t.Fatalf("write heartbeat: %v", err)
	}
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read heartbeat ack: %v", err)
	}
	var ack struct{ Cmd string }
	if json.Unmarshal(data, &ack) != nil || ack.Cmd != extmsg.CmdHeartbeatAck {
		t.Errorf("expected heartbeat_ack, got %s", data)
	}

	// 5) 业务消息：上行 -> 逻辑服 -> 下行
	payload := map[string]any{"cmd": "ping", "uid": uint64(10001), "data": "integration_test"}
	body, _ := json.Marshal(payload)
	if err := conn.WriteMessage(websocket.TextMessage, body); err != nil {
		t.Fatalf("write ping: %v", err)
	}
	_, data, err = conn.ReadMessage()
	if err != nil {
		t.Fatalf("read logic response: %v", err)
	}
	// 网关对客户端只下发 DownMsg.Data（逻辑服返回的 JSON），不是完整 DownMsg
	var logicResp struct {
		ConnID uint64 `json:"conn_id"`
		Msg    string `json:"msg"`
		Data   string `json:"data"`
	}
	if json.Unmarshal(data, &logicResp) != nil {
		t.Fatalf("invalid logic response: %s", data)
	}
	if logicResp.ConnID == 0 {
		t.Error("logic response conn_id should be non-zero")
	}
	if logicResp.Msg != "逻辑服已接收" {
		t.Errorf("logic response msg=%q", logicResp.Msg)
	}
	if !strings.Contains(logicResp.Data, "integration_test") {
		t.Errorf("logic response data should contain integration_test, got %q", logicResp.Data)
	}

	// 6) 关闭连接（网关会向逻辑服发 client_disconnect）
	_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1000, ""))
	time.Sleep(200 * time.Millisecond)
}
