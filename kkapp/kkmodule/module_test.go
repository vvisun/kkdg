package kkmodule

import (
	"errors"
	"testing"
)

// TestModule 用于测试的模块实现
type TestModule struct {
	Module
	initCalled    bool
	releaseCalled bool
	initError     error
}

func NewTestModule() *TestModule {
	m := &TestModule{}
	m.self = m
	m.ancestor = m
	m.descendants = make(map[uint32]IModule)
	m.descendants[0] = m // 根模块自己的ID为0
	return m
}

func (m *TestModule) OnInit() error {
	m.initCalled = true
	return m.initError
}

func (m *TestModule) OnRelease() {
	m.releaseCalled = true
}

// TestModule_GetModuleName 测试获取模块名称
func TestModule_GetModuleName(t *testing.T) {
	m := NewTestModule()

	// 初始名称应该为空
	if m.GetModuleName() != "" {
		t.Errorf("GetModuleName() = %s, want empty string", m.GetModuleName())
	}
}

func TestModule_FullName(t *testing.T) {
	m := NewTestModule()
	child := NewTestModule()
	_, err := m.AddModule(child)
	if err != nil {
		t.Fatalf("AddModule() error = %v", err)
	}
	if child.FullName() != ".TestModule" {
		t.Errorf("FullName() = %s, want .TestModule", child.FullName())
	}
}

// TestModule_AddModule 测试添加模块
func TestModule_AddModule(t *testing.T) {
	root := NewTestModule()

	child := NewTestModule()
	moduleId, err := root.AddModule(child)
	if err != nil {
		t.Fatalf("AddModule() error = %v", err)
	}
	if moduleId == 0 {
		t.Error("AddModule() should return non-zero moduleId")
	}

	// 验证子模块的属性
	if child.GetParent() != root {
		t.Error("child.GetParent() should return root")
	}
	if child.GetAncestor() != root {
		t.Error("child.GetAncestor() should return root")
	}
	if child.GetModuleName() == "" {
		t.Error("child.GetModuleName() should not be empty")
	}
	if !child.initCalled {
		t.Error("child.OnInit() should be called")
	}

	// 验证可以通过ID查找模块
	found := root.GetModule(moduleId)
	if found != child {
		t.Error("GetModule() should return the added child")
	}
}

// TestModule_AddModule_WithExistingId 测试添加已有ID的模块
func TestModule_AddModule_WithExistingId(t *testing.T) {
	root := NewTestModule()

	child1 := NewTestModule()
	_, err := root.AddModule(child1)
	if err != nil {
		t.Fatalf("AddModule(child1) error = %v", err)
	}
}

// TestModule_AddModule_OnInitError 测试OnInit失败的情况
func TestModule_AddModule_OnInitError(t *testing.T) {
	root := NewTestModule()

	child := NewTestModule()
	child.initError = errors.New("init failed")

	moduleId, err := root.AddModule(child)
	if err == nil {
		t.Error("AddModule() should return error when OnInit fails")
	}
	if moduleId != 0 {
		t.Error("AddModule() should return 0 when OnInit fails")
	}

	// 验证模块没有被添加到descendants
	found := root.GetModule(child.GetModuleId())
	if found != nil {
		t.Error("GetModule() should return nil for failed module")
	}
}

// TestModule_ReleaseModule 测试释放模块
func TestModule_ReleaseModule(t *testing.T) {
	root := NewTestModule()

	child := NewTestModule()
	moduleId, err := root.AddModule(child)
	if err != nil {
		t.Fatalf("AddModule() error = %v", err)
	}

	// 释放模块
	root.ReleaseModule(moduleId)

	// 验证模块已被释放
	if !child.releaseCalled {
		t.Error("child.OnRelease() should be called")
	}

	// 验证模块无法再被找到
	found := root.GetModule(moduleId)
	if found != nil {
		t.Error("GetModule() should return nil after ReleaseModule")
	}
}

// TestModule_ReleaseModule_WithChildren 测试释放有子模块的模块
func TestModule_ReleaseModule_WithChildren(t *testing.T) {
	root := NewTestModule()

	parent := NewTestModule()
	parentId, err := root.AddModule(parent)
	if err != nil {
		t.Fatalf("AddModule(parent) error = %v", err)
	}

	child1 := NewTestModule()
	child1Id, err := parent.AddModule(child1)
	if err != nil {
		t.Fatalf("AddModule(child1) error = %v", err)
	}

	child2 := NewTestModule()
	child2Id, err := parent.AddModule(child2)
	if err != nil {
		t.Fatalf("AddModule(child2) error = %v", err)
	}

	// 释放父模块，应该同时释放所有子模块
	root.ReleaseModule(parentId)

	// 验证所有模块都被释放
	if !parent.releaseCalled {
		t.Error("parent.OnRelease() should be called")
	}
	if !child1.releaseCalled {
		t.Error("child1.OnRelease() should be called")
	}
	if !child2.releaseCalled {
		t.Error("child2.OnRelease() should be called")
	}

	// 验证所有模块都无法再被找到
	if root.GetModule(parentId) != nil {
		t.Error("GetModule(parentId) should return nil")
	}
	if root.GetModule(child1Id) != nil {
		t.Error("GetModule(child1Id) should return nil")
	}
	if root.GetModule(child2Id) != nil {
		t.Error("GetModule(child2Id) should return nil")
	}
}

// TestModule_GetModule 测试查找模块
func TestModule_GetModule(t *testing.T) {
	root := NewTestModule()

	// 查找不存在的模块
	found := root.GetModule(999)
	if found != nil {
		t.Error("GetModule(999) should return nil")
	}

	// 添加模块后查找
	child := NewTestModule()
	moduleId, err := root.AddModule(child)
	if err != nil {
		t.Fatalf("AddModule() error = %v", err)
	}

	found = root.GetModule(moduleId)
	if found != child {
		t.Error("GetModule() should return the added module")
	}

	// 子模块也应该能查找到
	found = child.GetModule(moduleId)
	if found != child {
		t.Error("child.GetModule() should also find the module")
	}
}

// TestModule_GetAncestor 测试获取祖先模块
func TestModule_GetAncestor(t *testing.T) {
	root := NewTestModule()

	child := NewTestModule()
	_, err := root.AddModule(child)
	if err != nil {
		t.Fatalf("AddModule() error = %v", err)
	}

	if child.GetAncestor() != root {
		t.Error("child.GetAncestor() should return root")
	}

	if root.GetAncestor() != root {
		t.Error("root.GetAncestor() should return root itself")
	}
}

// TestModule_GetParent 测试获取父模块
func TestModule_GetParent(t *testing.T) {
	root := NewTestModule()

	child := NewTestModule()
	_, err := root.AddModule(child)
	if err != nil {
		t.Fatalf("AddModule() error = %v", err)
	}

	if child.GetParent() != root {
		t.Error("child.GetParent() should return root")
	}

	if root.GetParent() != nil {
		t.Error("root.GetParent() should return nil")
	}
}

// TestModule_OnInit 测试初始化
func TestModule_OnInit(t *testing.T) {
	m := NewTestModule()

	err := m.OnInit()
	if err != nil {
		t.Errorf("OnInit() error = %v, want nil", err)
	}
}

// TestModule_OnRelease 测试释放
func TestModule_OnRelease(t *testing.T) {
	m := NewTestModule()

	m.OnRelease()
	// OnRelease 是空实现，不会出错
}

// TestModule_MultipleLevels 测试多层级模块结构
func TestModule_MultipleLevels(t *testing.T) {
	root := NewTestModule()

	// 第一层
	level1 := NewTestModule()
	level1Id, err := root.AddModule(level1)
	if err != nil {
		t.Fatalf("AddModule(level1) error = %v", err)
	}

	// 第二层
	level2 := NewTestModule()
	level2Id, err := level1.AddModule(level2)
	if err != nil {
		t.Fatalf("AddModule(level2) error = %v", err)
	}

	// 第三层
	level3 := NewTestModule()
	level3Id, err := level2.AddModule(level3)
	if err != nil {
		t.Fatalf("AddModule(level3) error = %v", err)
	}

	// 验证层级关系
	if level1.GetParent() != root {
		t.Error("level1.GetParent() should return root")
	}
	if level2.GetParent() != level1 {
		t.Error("level2.GetParent() should return level1")
	}
	if level3.GetParent() != level2 {
		t.Error("level3.GetParent() should return level2")
	}

	// 所有模块的祖先都应该是root
	if level1.GetAncestor() != root {
		t.Error("level1.GetAncestor() should return root")
	}
	if level2.GetAncestor() != root {
		t.Error("level2.GetAncestor() should return root")
	}
	if level3.GetAncestor() != root {
		t.Error("level3.GetAncestor() should return root")
	}

	// 从root应该能找到所有模块
	if root.GetModule(level1Id) != level1 {
		t.Error("root.GetModule(level1Id) should return level1")
	}
	if root.GetModule(level2Id) != level2 {
		t.Error("root.GetModule(level2Id) should return level2")
	}
	if root.GetModule(level3Id) != level3 {
		t.Error("root.GetModule(level3Id) should return level3")
	}

	// 释放中间层，应该同时释放子层
	root.ReleaseModule(level1Id)

	if root.GetModule(level1Id) != nil {
		t.Error("GetModule(level1Id) should return nil after release")
	}
	if root.GetModule(level2Id) != nil {
		t.Error("GetModule(level2Id) should return nil after release")
	}
	if root.GetModule(level3Id) != nil {
		t.Error("GetModule(level3Id) should return nil after release")
	}
}

// TestModule_NewModuleId 测试模块ID生成
func TestModule_NewModuleId(t *testing.T) {
	root := NewTestModule()

	// 添加多个模块，验证ID递增
	child1 := NewTestModule()
	id1, err := root.AddModule(child1)
	if err != nil {
		t.Fatalf("AddModule(child1) error = %v", err)
	}

	child2 := NewTestModule()
	id2, err := root.AddModule(child2)
	if err != nil {
		t.Fatalf("AddModule(child2) error = %v", err)
	}

	child3 := NewTestModule()
	id3, err := root.AddModule(child3)
	if err != nil {
		t.Fatalf("AddModule(child3) error = %v", err)
	}

	// ID应该是递增的
	if id2 <= id1 {
		t.Errorf("id2 (%d) should be greater than id1 (%d)", id2, id1)
	}
	if id3 <= id2 {
		t.Errorf("id3 (%d) should be greater than id2 (%d)", id3, id2)
	}
}

// TestModule_ReleaseModule_NonExistent 测试释放不存在的模块
func TestModule_ReleaseModule_NonExistent(t *testing.T) {
	root := NewTestModule()

	// 释放不存在的模块会导致panic（当前实现的行为）
	// 使用recover来捕获panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("ReleaseModule(999) should not panic when module does not exist, but got panic: %v", r)
			}
		}()
		root.ReleaseModule(999)
	}()
}

// TestModule_GetBaseModule 测试获取基础模块
func TestModule_GetBaseModule(t *testing.T) {
	m := NewTestModule()

	base := m.getBaseModule()
	// getBaseModule 返回 IModule 接口，需要转换为具体类型进行比较
	if baseModule, ok := base.(*Module); ok {
		if baseModule != &m.Module {
			t.Error("getBaseModule() should return the base Module")
		}
	} else {
		t.Error("getBaseModule() should return *Module type")
	}
}
