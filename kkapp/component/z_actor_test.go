package component

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
)

// pingMsg is a simple test message for actor components.
type pingMsg struct{}

// actorComponent is a test component that uses the application's actor system.
// It demonstrates that a component can be treated as an actor-like unit whose
// behavior does not depend on which node (Application) it is attached to.
type actorComponent struct {
	Component
	pid      *actor.PID
	recvCnt  atomic.Int32
	nodeSeen atomic.Value // string
}

func (c *actorComponent) Init() error {
	app := c.GetApplication()
	if app == nil {
		return nil
	}
	sys := app.GetActorSystem()
	props := actor.PropsFromFunc(func(ctx actor.Context) {
		switch ctx.Message().(type) {
		case pingMsg:
			c.nodeSeen.Store(app.GetNodeId())
			c.recvCnt.Add(1)
		}
	})
	c.pid = sys.Root.Spawn(props)
	return nil
}

func (c *actorComponent) Start() error { return nil }

func (c *actorComponent) Stop() error {
	if c.pid != nil {
		// Best-effort synchronous stop for the actor
		_ = c.GetApplication().GetActorSystem().Root.StopFuture(c.pid).Wait()
	}
	return nil
}

// TestComponent_ActorAcrossNodes verifies the guarantees described in doc.go:
// - The same component type can be attached to different nodes (applications)
//   without changing its logic.
// - Moving the component between nodes only changes the node identity; the
//   actor behavior stays the same.
func TestComponent_ActorAcrossNodes(t *testing.T) {
	// node1: attach component and send ping
	node1 := kkapp.NewNodeInfo("node1", "logic", "a1", "", nil)
	app1 := NewApplication(node1)
	comp1 := &actorComponent{Component: Component{id: "comp"}}
	if err := app1.AddComponent(comp1); err != nil {
		t.Fatalf("AddComponent(app1): %v", err)
	}
	if err := app1.Start(); err != nil {
		t.Fatalf("Start(app1): %v", err)
	}
	if comp1.pid == nil {
		t.Fatalf("actorComponent on app1 did not spawn actor PID")
	}
	app1.GetActorSystem().Root.Send(comp1.pid, pingMsg{})
	time.Sleep(20 * time.Millisecond)
	if got := comp1.recvCnt.Load(); got != 1 {
		t.Fatalf("comp1.recvCnt = %d, want 1", got)
	}
	if v := comp1.nodeSeen.Load(); v == nil || v.(string) != "node1" {
		t.Fatalf("comp1.nodeSeen = %v, want node1", v)
	}

	// node2: same component type, same ID, attached to another node.
	node2 := kkapp.NewNodeInfo("node2", "logic", "a2", "", nil)
	app2 := NewApplication(node2)
	comp2 := &actorComponent{Component: Component{id: "comp"}}
	if err := app2.AddComponent(comp2); err != nil {
		t.Fatalf("AddComponent(app2): %v", err)
	}
	if err := app2.Start(); err != nil {
		t.Fatalf("Start(app2): %v", err)
	}
	if comp2.pid == nil {
		t.Fatalf("actorComponent on app2 did not spawn actor PID")
	}
	app2.GetActorSystem().Root.Send(comp2.pid, pingMsg{})
	time.Sleep(20 * time.Millisecond)
	if got := comp2.recvCnt.Load(); got != 1 {
		t.Fatalf("comp2.recvCnt = %d, want 1", got)
	}
	if v := comp2.nodeSeen.Load(); v == nil || v.(string) != "node2" {
		t.Fatalf("comp2.nodeSeen = %v, want node2", v)
	}

	// Clean up
	_ = app1.Stop()
	_ = app2.Stop()
}

// routerComponent forwards messages from one actor to another inside the same node.
// This exercises actor-to-actor communication using the application's actor system,
// which is the building block for cross-node messaging when combined with remotes.
type routerComponent struct {
	Component
	pid *actor.PID
}

type forwardMsg struct {
	Target *actor.PID
	Inner  any
}

func (c *routerComponent) Init() error {
	app := c.GetApplication()
	if app == nil {
		return nil
	}
	sys := app.GetActorSystem()
	props := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case forwardMsg:
			if msg.Target != nil && msg.Inner != nil {
				ctx.Send(msg.Target, msg.Inner)
			}
		}
	})
	c.pid = sys.Root.Spawn(props)
	return nil
}

func (c *routerComponent) Start() error { return nil }

func (c *routerComponent) Stop() error {
	if c.pid != nil {
		_ = c.GetApplication().GetActorSystem().Root.StopFuture(c.pid).Wait()
	}
	return nil
}

// proxyComponent 模拟“跨物理节点”的 actor 通信：它本身挂在 node1 上，
// 收到 crossNodeMsg 后，会通过 sendRemote 回调将消息发送到其它节点的 actorSystem。
type proxyComponent struct {
	Component
	pid        *actor.PID
	sendRemote func(targetNode string, msg any)
}

type crossNodeMsg struct {
	TargetNode string
	Inner      any
}

func (c *proxyComponent) Init() error {
	app := c.GetApplication()
	if app == nil {
		return nil
	}
	sys := app.GetActorSystem()
	props := actor.PropsFromFunc(func(ctx actor.Context) {
		switch msg := ctx.Message().(type) {
		case crossNodeMsg:
			if c.sendRemote != nil && msg.TargetNode != "" && msg.Inner != nil {
				c.sendRemote(msg.TargetNode, msg.Inner)
			}
		}
	})
	c.pid = sys.Root.Spawn(props)
	return nil
}

func (c *proxyComponent) Start() error { return nil }

func (c *proxyComponent) Stop() error {
	if c.pid != nil {
		_ = c.GetApplication().GetActorSystem().Root.StopFuture(c.pid).Wait()
	}
	return nil
}

// TestComponent_ActorMessageRouting verifies that components treated as actors
// can communicate via the application's actor system: one component forwards a
// message to another, and the receiver observes the correct node identity.
func TestComponent_ActorMessageRouting(t *testing.T) {
	node := kkapp.NewNodeInfo("nodeX", "logic", "ax", "", nil)
	app := NewApplication(node)

	rc := &routerComponent{Component: Component{id: "router"}}
	ac := &actorComponent{Component: Component{id: "worker"}}

	if err := app.AddComponent(rc); err != nil {
		t.Fatalf("AddComponent(router): %v", err)
	}
	if err := app.AddComponent(ac); err != nil {
		t.Fatalf("AddComponent(worker): %v", err)
	}
	if err := app.Start(); err != nil {
		t.Fatalf("Start(app): %v", err)
	}
	if rc.pid == nil || ac.pid == nil {
		t.Fatalf("router or worker PID is nil: router=%v worker=%v", rc.pid, ac.pid)
	}

	app.GetActorSystem().Root.Send(rc.pid, forwardMsg{
		Target: ac.pid,
		Inner:  pingMsg{},
	})
	time.Sleep(20 * time.Millisecond)

	if got := ac.recvCnt.Load(); got != 1 {
		t.Fatalf("ac.recvCnt = %d, want 1", got)
	}
	if v := ac.nodeSeen.Load(); v == nil || v.(string) != "nodeX" {
		t.Fatalf("ac.nodeSeen = %v, want nodeX", v)
	}

	_ = app.Stop()
}

// TestComponent_ActorCrossPhysicalNodes 演示“跨物理节点”的通信模型：
// - nodeA 上挂 proxyComponent，nodeB 上挂 actorComponent。
// - proxyComponent 收到 crossNodeMsg 后，通过 sendRemote 回调将消息转发到 nodeB 的 actorSystem。
// 这与实际部署中“通过 remotes/cluster 将 actor 消息路由到另一台物理机”的模式一致。
func TestComponent_ActorCrossPhysicalNodes(t *testing.T) {
	// nodeA: proxy 节点
	nodeA := kkapp.NewNodeInfo("nodeA", "logic", "aA", "", nil)
	appA := NewApplication(nodeA)

	// nodeB: 目标 actor 节点
	nodeB := kkapp.NewNodeInfo("nodeB", "logic", "aB", "", nil)
	appB := NewApplication(nodeB)
	acB := &actorComponent{Component: Component{id: "workerB"}}
	if err := appB.AddComponent(acB); err != nil {
		t.Fatalf("AddComponent(appB): %v", err)
	}
	if err := appB.Start(); err != nil {
		t.Fatalf("Start(appB): %v", err)
	}
	if acB.pid == nil {
		t.Fatalf("actorComponent on appB did not spawn actor PID")
	}

	// 在 appA 上挂 proxyComponent，通过 sendRemote 回调把消息发到 appB 的 actorSystem。
	proxy := &proxyComponent{
		Component: Component{id: "proxy"},
	}
	proxy.sendRemote = func(targetNode string, msg any) {
		if targetNode != "nodeB" {
			return
		 }
		appB.GetActorSystem().Root.Send(acB.pid, msg)
	}
	if err := appA.AddComponent(proxy); err != nil {
		t.Fatalf("AddComponent(appA): %v", err)
	}
	if err := appA.Start(); err != nil {
		t.Fatalf("Start(appA): %v", err)
	}
	if proxy.pid == nil {
		t.Fatalf("proxyComponent did not spawn actor PID")
	}

	// 从 nodeA 发送“跨节点”消息，目标为 nodeB。
	appA.GetActorSystem().Root.Send(proxy.pid, crossNodeMsg{
		TargetNode: "nodeB",
		Inner:      pingMsg{},
	})
	time.Sleep(40 * time.Millisecond)

	if got := acB.recvCnt.Load(); got != 1 {
		t.Fatalf("acB.recvCnt = %d, want 1", got)
	}
	if v := acB.nodeSeen.Load(); v == nil || v.(string) != "nodeB" {
		t.Fatalf("acB.nodeSeen = %v, want nodeB", v)
	}

	_ = appA.Stop()
	_ = appB.Stop()
}



