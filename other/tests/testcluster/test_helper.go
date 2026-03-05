package testcluster

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
)

// startTestNatsServer 返回测试用的NATS服务器地址
// 注意：假设NATS服务器已经在本地运行（默认地址：nats://127.0.0.1:4222）
// 如果NATS服务器运行在其他地址，可以修改defaultNatsAddress或传入自定义地址
func startTestNatsServer() (interface{}, string, error) {
	// 使用默认地址；如果本地没有运行 NATS，则返回 error（测试会 Skip）
	url := "nats://127.0.0.1:4222"
	nc, err := nats.Connect(url, nats.Timeout(500*time.Millisecond))
	if err != nil {
		return nil, url, err
	}
	nc.Close()
	return nil, url, nil
}

// waitForMembers 等待成员出现
func waitForMembers(d kkdiscovery.IDiscovery, count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		members := d.GetMemberMgr().MemberCount()
		if members >= count {
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
