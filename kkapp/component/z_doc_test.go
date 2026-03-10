package component

import (
	"testing"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/vvisun/kkdg/kkapp"
)

// doc 测试用组件：可指定名称，便于模拟 comp1/comp2/comp3
type docTestComp struct {
	Component
	name string
}

func (c *docTestComp) GetCompName() string { return c.name }

func (c *docTestComp) OnInit() error  { return nil }
func (c *docTestComp) OnStart() error { return nil }
func (c *docTestComp) OnStop() error  { return nil }

func (c *docTestComp) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *actor.Started:
		_ = c.OnStart()
	case *actor.Stopping:
		_ = c.OnStop()
	}
}

// TestDoc_ApplicationIsOneNode 验证：每个应用程序视为 1 个节点。
func TestDoc_ApplicationIsOneNode(t *testing.T) {
	nodeInfo := kkapp.NewNodeInfo("node1", "gate", "127.0.0.1:8080", "", nil)
	app := NewApplication(nodeInfo, nil)

	if app.GetNodeId() != "node1" {
		t.Errorf("GetNodeId() = %s, want node1", app.GetNodeId())
	}
	if app.GetNodeType() != "gate" {
		t.Errorf("GetNodeType() = %s, want gate", app.GetNodeType())
	}
	info := app.GetNodeInfo()
	if info == nil || info.GetNodeId() != "node1" {
		t.Errorf("GetNodeInfo() should return same node identity")
	}
}

// TestDoc_NodeCanHaveMultipleComponents 验证：每个节点可以包含多个组件。
func TestDoc_NodeCanHaveMultipleComponents(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)

	if err := app.AddComponent(&docTestComp{name: "comp1"}); err != nil {
		t.Fatalf("add comp1: %v", err)
	}
	if err := app.AddComponent(&docTestComp{name: "comp2"}); err != nil {
		t.Fatalf("add comp2: %v", err)
	}
	if err := app.AddComponent(&docTestComp{name: "comp3"}); err != nil {
		t.Fatalf("add comp3: %v", err)
	}

	if err := app.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		time.Sleep(10 * time.Millisecond)
		_ = app.Stop()
	}()

	time.Sleep(30 * time.Millisecond)

	// 同一节点下多个组件都应能通过名称拿到 PID
	for _, name := range []string{"comp1", "comp2", "comp3"} {
		pid := app.GetCompPID(name)
		if pid == nil {
			t.Errorf("GetChildPID(%q) = nil, want non-nil", name)
		}
	}
}

// TestDoc_ComponentIsActor 验证：每个组件视为 1 个 actor（可寻址、有 PID）。
func TestDoc_ComponentIsActor(t *testing.T) {
	app := NewApplication(kkapp.NewNodeInfo("node1", "test", "127.0.0.1:8080", "", nil), nil)
	comp := &docTestComp{name: "logic"}
	if err := app.AddComponent(comp); err != nil {
		t.Fatalf("add component: %v", err)
	}
	if err := app.Start(); err != nil {
		t.Fatalf("start: %v", err)
	}
	defer func() {
		time.Sleep(10 * time.Millisecond)
		_ = app.Stop()
	}()

	time.Sleep(30 * time.Millisecond)

	// 组件应有 PID（由 Application 在 Started 时 spawn 并 setPID）
	if comp.GetPID() == nil {
		t.Error("component.GetPID() = nil, component should be an actor with PID")
	}
	// 通过节点（Application）按名称获取子 actor PID，逻辑层只需“给目标节点发消息”即可寻址
	if app.GetCompPID("logic") == nil {
		t.Error("GetChildPID(\"logic\") = nil, component should be addressable by name on node")
	}
}

// TestDoc_MultipleNodesInOneProcess 验证：同一进程内可运行多个节点（单机多节点部署）。
func TestDoc_MultipleNodesInOneProcess(t *testing.T) {
	node1 := NewApplication(kkapp.NewNodeInfo("node1", "gate", "127.0.0.1:8080", "", nil), nil)
	node2 := NewApplication(kkapp.NewNodeInfo("node2", "game", "127.0.0.1:8081", "", nil), nil)

	_ = node1.AddComponent(&docTestComp{name: "comp1"})
	_ = node2.AddComponent(&docTestComp{name: "comp2"})

	if err := node1.Start(); err != nil {
		t.Fatalf("node1 start: %v", err)
	}
	if err := node2.Start(); err != nil {
		t.Fatalf("node2 start: %v", err)
	}
	defer func() {
		time.Sleep(10 * time.Millisecond)
		_ = node1.Stop()
		_ = node2.Stop()
	}()

	time.Sleep(30 * time.Millisecond)

	// 逻辑层只关心“给目标节点发消息”：通过节点 + 组件名取 PID，与是否同进程无关
	pid1 := node1.GetCompPID("comp1")
	pid2 := node2.GetCompPID("comp2")
	if pid1 == nil || pid2 == nil {
		t.Fatalf("GetChildPID: node1.comp1=%v, node2.comp2=%v", pid1, pid2)
	}
	// 两节点各自有独立 ActorSystem，组件可被正确寻址（PID 来自不同 system，此处仅验证可寻址）
}

// TestDoc_DeploymentLayoutTransparency 验证：组件挂接方式可任意组合，逻辑层按“节点+组件名”寻址即可。
// doc 例：comp1,comp2,comp3 分别挂 node1,node2,node3 与 comp1+comp2 挂 node1、comp3 挂 node2 效果等价。
func TestDoc_DeploymentLayoutTransparency(t *testing.T) {
	// 布局 A：3 个节点，每节点 1 个组件
	nodesA := []*Application{
		NewApplication(kkapp.NewNodeInfo("n1", "t", "127.0.0.1:9001", "", nil), nil),
		NewApplication(kkapp.NewNodeInfo("n2", "t", "127.0.0.1:9002", "", nil), nil),
		NewApplication(kkapp.NewNodeInfo("n3", "t", "127.0.0.1:9003", "", nil), nil),
	}
	_ = nodesA[0].AddComponent(&docTestComp{name: "comp1"})
	_ = nodesA[1].AddComponent(&docTestComp{name: "comp2"})
	_ = nodesA[2].AddComponent(&docTestComp{name: "comp3"})

	for _, app := range nodesA {
		if err := app.Start(); err != nil {
			t.Fatalf("layout A start: %v", err)
		}
	}
	time.Sleep(30 * time.Millisecond)

	getPIDs := func(apps []*Application, names []string) []*actor.PID {
		pids := make([]*actor.PID, len(names))
		for i, name := range names {
			pids[i] = apps[i].GetCompPID(name)
		}
		return pids
	}
	pidsA := getPIDs(nodesA, []string{"comp1", "comp2", "comp3"})
	for i, pid := range pidsA {
		if pid == nil {
			t.Errorf("layout A: GetChildPID(comp%d) = nil", i+1)
		}
	}

	for _, app := range nodesA {
		_ = app.Stop()
	}
	time.Sleep(20 * time.Millisecond)

	// 布局 B：2 个节点，node1 挂 comp1+comp2，node2 挂 comp3
	node1B := NewApplication(kkapp.NewNodeInfo("n1", "t", "127.0.0.1:9001", "", nil), nil)
	node2B := NewApplication(kkapp.NewNodeInfo("n2", "t", "127.0.0.1:9002", "", nil), nil)
	_ = node1B.AddComponent(&docTestComp{name: "comp1"})
	_ = node1B.AddComponent(&docTestComp{name: "comp2"})
	_ = node2B.AddComponent(&docTestComp{name: "comp3"})

	if err := node1B.Start(); err != nil {
		t.Fatalf("layout B node1 start: %v", err)
	}
	if err := node2B.Start(); err != nil {
		t.Fatalf("layout B node2 start: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	// 同样的寻址方式：通过“所在节点”+ 组件名取 PID，不关心组件实际分布在几个进程
	if node1B.GetCompPID("comp1") == nil || node1B.GetCompPID("comp2") == nil {
		t.Error("layout B: node1 should have comp1 and comp2")
	}
	if node2B.GetCompPID("comp3") == nil {
		t.Error("layout B: node2 should have comp3")
	}

	_ = node1B.Stop()
	_ = node2B.Stop()
}
