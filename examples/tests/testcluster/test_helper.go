package testcluster

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kknet/kkcluster"
)

// startTestNatsServer 返回测试用的NATS服务器地址
// 注意：假设NATS服务器已经在本地运行（默认地址：nats://127.0.0.1:4222）
// 如果NATS服务器运行在其他地址，可以修改defaultNatsAddress或传入自定义地址
func startTestNatsServer() (interface{}, string, error) {
	// 使用默认地址，假设NATS服务器已经在本地运行
	return nil, "nats://127.0.0.1:4222", nil
}

// waitForMembers 等待成员出现
func waitForMembers(d kkcluster.IDiscovery, count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		members := d.Map()
		if len(members) >= count {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// waitForConnection 等待NATS连接就绪
func waitForConnection(conn *nats.Conn, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if conn.IsConnected() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}
