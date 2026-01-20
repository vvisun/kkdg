package testactor

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vvisun/kkdg/kknet/kkactor"
	"github.com/vvisun/kkdg/kknet/kkcluster"
)

type actorFunc func(ctx kkactor.Context)

func (f actorFunc) Receive(ctx kkactor.Context) {
	f(ctx)
}

func TestActorRemote_Nats(t *testing.T) {
	_, natsURL, err := startTestNatsServer()
	if err != nil {
		t.Fatalf("failed to start NATS server: %v", err)
	}

	discoveryA := kkcluster.NewNatsDiscovery("test", "nodeA", "typeA", "127.0.0.1:10001", natsURL, nil)
	discoveryB := kkcluster.NewNatsDiscovery("test", "nodeB", "typeB", "127.0.0.1:10002", natsURL, nil)
	if err := discoveryA.Start(); err != nil {
		t.Fatalf("discoveryA start failed: %v", err)
	}
	if err := discoveryB.Start(); err != nil {
		discoveryA.Stop()
		t.Fatalf("discoveryB start failed: %v", err)
	}
	t.Cleanup(func() {
		discoveryA.Stop()
		discoveryB.Stop()
	})

	clusterA := kkcluster.NewNatsCluster("nodeA", discoveryA, natsURL)
	clusterB := kkcluster.NewNatsCluster("nodeB", discoveryB, natsURL)
	if err := clusterA.Init(); err != nil {
		t.Fatalf("clusterA init failed: %v", err)
	}
	if err := clusterB.Init(); err != nil {
		clusterA.Stop()
		t.Fatalf("clusterB init failed: %v", err)
	}
	t.Cleanup(func() {
		clusterA.Stop()
		clusterB.Stop()
	})

	if !waitForMembers(discoveryA, 1, 5*time.Second) || !waitForMembers(discoveryB, 1, 5*time.Second) {
		t.Fatalf("members not ready")
	}

	kkactor.RegisterMessageType("kkactor.test.remote.ping."+t.Name(), &ping{})
	kkactor.RegisterMessageType("kkactor.test.remote.pong."+t.Name(), &pong{})

	remoteA := kkactor.NewClusterRemote("nodeA", clusterA)
	remoteB := kkactor.NewClusterRemote("nodeB", clusterB)

	sysA := kkactor.NewActorSystemWithRemote("nodeA", remoteA, nil)
	sysB := kkactor.NewActorSystemWithRemote("nodeB", remoteB, nil)

	received := make(chan string, 1)
	pidB := sysB.Spawn(kkactor.FromProducer(func() kkactor.Actor {
		return actorFunc(func(ctx kkactor.Context) {
			switch msg := ctx.Message().(type) {
			case *ping:
				if msg.Value != "" {
					received <- msg.Value
				}
				ctx.Respond(&pong{Value: msg.Value})
			}
		})
	}))
	if pidB == nil {
		t.Fatalf("spawn returned nil pid")
	}

	rootA := kkactor.NewRootContext(sysA)
	remotePID := kkactor.NewRemotePID("nodeB", pidB.ID())

	rootA.Send(remotePID, &ping{Value: "pub"})
	select {
	case value := <-received:
		if value != "pub" {
			t.Fatalf("unexpected publish value: %s", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for publish")
	}

	fut := rootA.RequestFuture(remotePID, &ping{Value: "req"}, 3*time.Second)
	msg, err := fut.Result()
	if err != nil {
		t.Fatalf("remote request failed: %v", err)
	}
	resp, ok := msg.(*pong)
	if !ok {
		t.Fatalf("unexpected response type: %T", msg)
	}
	if resp.Value != "req" {
		t.Fatalf("unexpected response value: %s", resp.Value)
	}
}

type ping struct {
	Value string
}

type pong struct {
	Value string
}

// startTestNatsServer returns test NATS server address.
// NATS server must already be running locally.
func startTestNatsServer() (interface{}, string, error) {
	return nil, "nats://127.0.0.1:4222", nil
}

// waitForMembers waits until discovery sees expected members.
func waitForMembers(d kkcluster.IDiscovery, count int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(d.Map()) >= count {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

// waitForConnection waits for NATS connection ready.
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
