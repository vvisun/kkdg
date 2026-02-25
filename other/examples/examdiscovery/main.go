package main

import (
	"fmt"
	"time"

	"github.com/vvisun/kkdg/kkapp"
	"github.com/vvisun/kkdg/remotes/kkdiscovery"
	"github.com/vvisun/kkdg/remotes/kkdiscovery/dnats"
)

// examdiscovery 演示基于 NATS 的服务发现用法
// cd other/examples/examdiscovery; go run main.go
// 需先启动 NATS（默认 nats://127.0.0.1:4222）

func main() {
	natsURL := "nats://127.0.0.1:4222"

	nodeInfo1 := kkapp.NewNodeInfo("node1", "gate", "127.0.0.1:8080", "", nil)
	nodeInfo2 := kkapp.NewNodeInfo("node2", "gate", "127.0.0.1:8081", "", nil)

	opts := dnats.ApplyNatsOptions(dnats.WithUrl(natsURL))

	discovery1 := dnats.NewNatsDiscovery("exam1", nodeInfo1, nil, opts)
	discovery2 := dnats.NewNatsDiscovery("exam2", nodeInfo2, nil, opts)

	// 监听成员添加
	discovery1.OnAddMember(func(member kkdiscovery.IMember) {
		fmt.Printf("[node1] member added: %s (%s) @ %s\n",
			member.GetNodeID(), member.GetNodeType(), member.GetAddress())
	})
	discovery1.OnRemoveMember(func(member kkdiscovery.IMember) {
		fmt.Printf("[node1] member removed: %s\n", member.GetNodeID())
	})

	if err := discovery1.Start(); err != nil {
		fmt.Printf("discovery1.Start() failed: %v\n", err)
		return
	}
	defer discovery1.Stop()

	if err := discovery2.Start(); err != nil {
		fmt.Printf("discovery2.Start() failed: %v\n", err)
		return
	}
	defer discovery2.Stop()

	// 等待相互发现
	if !waitForMembers(discovery1, 1, 5*time.Second) {
		fmt.Println("discovery1 did not discover node2 (is NATS running?)")
		return
	}
	fmt.Println("--- mutual discovery ok ---")

	// 按类型列出成员
	gates := discovery1.ListByType("gate")
	fmt.Printf("ListByType(gate): %d members\n", len(gates))
	for _, m := range gates {
		fmt.Printf("  - %s @ %s\n", m.GetNodeID(), m.GetAddress())
	}

	// 随机获取一个
	if rnd, ok := discovery1.Random("gate"); ok {
		fmt.Printf("Random(gate): %s\n", rnd.GetNodeID())
	}

	// 获取指定成员
	if m, ok := discovery1.GetMember("node2"); ok {
		fmt.Printf("GetMember(node2): type=%s, addr=%s\n", m.GetNodeType(), m.GetAddress())
	}

	// 统计
	stats := discovery1.Stats()
	fmt.Printf("\n=== Discovery Stats ===\n")
	fmt.Printf("MemberCount: %d\n", stats.MemberCount)
	fmt.Printf("MembersAdded: %d\n", stats.MembersAdded)
	fmt.Printf("MembersRemoved: %d\n", stats.MembersRemoved)
	fmt.Printf("HeartbeatsSent: %d\n", stats.HeartbeatsSent)
	fmt.Printf("IsConnected: %v\n", stats.IsConnected)

	fmt.Println("\nexamdiscovery demo ok")
}

func waitForMembers(d kkdiscovery.IDiscovery, count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if d.MemberCount() >= count {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}
