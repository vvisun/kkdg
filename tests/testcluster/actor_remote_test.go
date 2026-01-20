package testcluster

import (
	"testing"
	"time"

	"github.com/vvisun/kkdg/kknet/kkactor"
	"github.com/vvisun/kkdg/kknet/kkcluster"
)

// 基于 kkcluster 的远程 Actor 的 Tell + Ask 集成测试。
func TestClusterRemote_ActorTellAndAsk(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Fatalf("Failed to start NATS server: %v", err)
	}

	// 创建两个服务发现与集群节点：node1 (客户端) 和 node2 (服务端)，同一类型。
	discovery1 := kkcluster.NewNatsDiscovery("test1", "node1", "type1", "127.0.0.1:9001", natsURL, nil)
	discovery2 := kkcluster.NewNatsDiscovery("test2", "node2", "type1", "127.0.0.1:9002", natsURL, nil)

	if err := discovery1.Start(); err != nil {
		t.Fatalf("discovery1.Start() failed: %v", err)
	}
	defer discovery1.Stop()

	if err := discovery2.Start(); err != nil {
		t.Fatalf("discovery2.Start() failed: %v", err)
	}
	defer discovery2.Stop()

	// 等待彼此发现
	if !waitForMembers(discovery1, 1, 3*time.Second) {
		t.Fatal("discovery1 did not discover node2")
	}

	// 创建 NatsCluster
	cluster1 := kkcluster.NewNatsCluster("node1", discovery1, natsURL)
	cluster2 := kkcluster.NewNatsCluster("node2", discovery2, natsURL)

	if err := cluster1.Init(); err != nil {
		t.Fatalf("cluster1.Init() failed: %v", err)
	}
	defer cluster1.Stop()

	if err := cluster2.Init(); err != nil {
		t.Fatalf("cluster2.Init() failed: %v", err)
	}
	defer cluster2.Stop()

	// 创建两个 ActorSystem
	sys1 := kkactor.NewActorSystem()
	defer sys1.Stop()
	sys2 := kkactor.NewActorSystem()
	defer sys2.Stop()

	// 创建并启动 ClusterRemoteSystem
	rs1 := kkactor.NewClusterRemoteSystem("node1", cluster1, discovery1)
	if err := rs1.Start(sys1); err != nil {
		t.Fatalf("rs1.Start() failed: %v", err)
	}
	sys1.SetRemoteSystem(rs1)
	defer rs1.Stop()

	rs2 := kkactor.NewClusterRemoteSystem("node2", cluster2, discovery2)
	if err := rs2.Start(sys2); err != nil {
		t.Fatalf("rs2.Start() failed: %v", err)
	}
	sys2.SetRemoteSystem(rs2)
	defer rs2.Stop()

	// 在 node2 上启动一个处理二进制消息并回显的 actor
	recv := make(chan []byte, 1)
	serverPID := sys2.Spawn(kkactor.PropsFromFunc(func(ctx kkactor.IContext) {
		switch m := ctx.Message().(type) {
		case []byte:
			// 记录收到的消息
			select {
			case recv <- m:
			default:
			}
			// 如果有 sender，则回显
			if sender := ctx.Sender(); sender != nil {
				resp := append([]byte("echo:"), m...)
				ctx.Send(sender, resp)
			}
		}
	}))
	if serverPID == nil {
		t.Fatal("serverPID is nil")
	}

	// 在 node1 上构造指向 node2 上 actor 的远程 PID
	remotePID := kkactor.NewRemotePID("node2", serverPID.ActorID(), sys1)
	if remotePID == nil {
		t.Fatal("NewRemotePID returned nil")
	}

	// 测试远程单向发送（Tell）
	t.Run("Tell", func(t *testing.T) {
		payload := []byte("hello")
		remotePID.Tell(payload)

		select {
		case got := <-recv:
			if string(got) != "hello" {
				t.Fatalf("server received %q, want %q", string(got), "hello")
			}
		case <-time.After(2 * time.Second):
			t.Fatal("server did not receive message via remote Tell")
		}
	})

	// 测试远程 Ask/Reply
	t.Run("Ask", func(t *testing.T) {
		root := kkactor.NewRootContext(sys1)
		resp, ok := root.Ask(remotePID, []byte("world"), 2*time.Second)
		if !ok {
			t.Fatal("expected reply from remote actor, got timeout")
		}
		b, ok := resp.([]byte)
		if !ok {
			t.Fatalf("expected []byte reply, got %T", resp)
		}
		if string(b) != "echo:world" {
			t.Fatalf("unexpected reply %q, want %q", string(b), "echo:world")
		}
	})
}
