package testcluster

import (
	"fmt"
	"testing"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkcluster"
	"github.com/vvisun/kkdg/remotes/kkcluster/cnats"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
)

// ExampleStats 统计信息使用示例
func TestStats(t *testing.T) {
	// 创建服务发现
	nodeInfo := kkapp.NewNodeInfo("node1", "typea", "127.0.0.1:8080", "", nil)
	discovery := dnats.NewNatsDiscovery("test", nodeInfo, dnats.ApplyNatsOptions(), kkdiscovery.ApplyOptions())

	// 启动服务发现
	if err := discovery.Start(); err != nil {
		fmt.Printf("Failed to start discovery: %v\n", err)
		return
	}
	defer discovery.Stop()

	// 创建集群
	cluster := cnats.NewNatsCluster("node1", "typea", discovery, cnats.ApplyNatsOptions(), kkcluster.ApplyOptions())
	if err := cluster.Start(); err != nil {
		fmt.Printf("Failed to init cluster: %v\n", err)
		return
	}
	defer cluster.Stop()

	// 等待一段时间
	time.Sleep(2 * time.Second)

	// 获取统计信息
	clusterStats := cluster.Stats()
	discoveryStats := discovery.Stats()

	// 打印集群统计信息
	fmt.Printf("=== Cluster Stats ===\n")
	fmt.Printf("Publish Sent: %d\n", clusterStats.PublishSent)
	fmt.Printf("Publish Received: %d\n", clusterStats.PublishReceived)
	fmt.Printf("Request Sent: %d\n", clusterStats.RequestSent)
	fmt.Printf("Request Received: %d\n", clusterStats.RequestReceived)
	fmt.Printf("Response Sent: %d\n", clusterStats.ResponseSent)
	fmt.Printf("Response Received: %d\n", clusterStats.ResponseReceived)
	fmt.Printf("Sent Bytes: %d\n", clusterStats.SentBytes)
	fmt.Printf("Received Bytes: %d\n", clusterStats.ReceivedBytes)
	fmt.Printf("Errors: %d\n", clusterStats.Errors)
	fmt.Printf("Reconnects: %d\n", clusterStats.Reconnects)
	fmt.Printf("Is Connected: %v\n", clusterStats.IsConnected)

	// 打印服务发现统计信息
	fmt.Printf("\n=== Discovery Stats ===\n")
	fmt.Printf("Member Count: %d\n", discoveryStats.MemberCount)
	fmt.Printf("Members Added: %d\n", discoveryStats.MembersAdded)
	fmt.Printf("Members Removed: %d\n", discoveryStats.MembersRemoved)
	fmt.Printf("Heartbeats Sent: %d\n", discoveryStats.HeartbeatsSent)
	fmt.Printf("Heartbeats Received: %d\n", discoveryStats.HeartbeatsReceived)
	fmt.Printf("Errors: %d\n", discoveryStats.Errors)
	fmt.Printf("Reconnects: %d\n", discoveryStats.Reconnects)
	fmt.Printf("Is Connected: %v\n", discoveryStats.IsConnected)
}
