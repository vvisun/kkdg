package module

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/vvisun/kkdg/utils/kklog"
)

type IModule interface {
	SetModuleId(moduleId uint32) bool
	GetModuleId() uint32
	GetModuleName() string

	AddModule(module IModule) (uint32, error)
	ReleaseModule(moduleId uint32)

	GetModule(moduleId uint32) IModule
	GetAncestor() IModule
	GetParent() IModule

	OnInit() error
	OnRelease()

	getBaseModule() IModule
	newModuleId() uint32
}

type Module struct {
	moduleId   uint32 //模块Id
	moduleName string //模块名称

	parent       IModule            //父亲
	self         IModule            //自己
	childs       []IModule          //孩子们
	ancestor     IModule            //始祖
	seedModuleId uint32             //模块id种子
	descendants  map[uint32]IModule //始祖的后裔们
}

var _ IModule = (*Module)(nil)

func (m *Module) AddModule(module IModule) (uint32, error) {
	pAddModule := module.getBaseModule().(*Module)
	if pAddModule.GetModuleId() == 0 {
		pAddModule.moduleId = m.newModuleId()
	}

	_, ok := m.ancestor.getBaseModule().(*Module).descendants[module.GetModuleId()]
	if ok {
		return 0, fmt.Errorf("exists module id %d", module.GetModuleId())
	}

	pAddModule.self = module
	pAddModule.parent = m.self
	pAddModule.ancestor = m.ancestor
	pAddModule.moduleName = reflect.Indirect(reflect.ValueOf(module)).Type().Name()

	m.childs = append(m.childs, module)
	m.ancestor.getBaseModule().(*Module).descendants[module.GetModuleId()] = module

	err := module.OnInit()
	if err != nil {
		delete(m.ancestor.getBaseModule().(*Module).descendants, module.GetModuleId())
		m.childs = m.childs[:len(m.childs)-1]
		kklog.Errorf("module OnInit error: %v", err)
		return 0, err
	}

	kklog.Debugf("Add module %s completed", module.GetModuleName())
	return module.GetModuleId(), nil
}

func (m *Module) ReleaseModule(moduleId uint32) {
	pModule := m.GetModule(moduleId).getBaseModule().(*Module)
	pModule.self.OnRelease()
	kklog.Debugf("Release module %s", pModule.GetModuleName())

	for i := len(pModule.childs) - 1; i >= 0; i-- {
		m.ReleaseModule(pModule.childs[i].GetModuleId())
	}

	m.childs = slices.DeleteFunc(m.childs, func(module IModule) bool {
		return module.GetModuleId() == moduleId
	})

	delete(m.ancestor.getBaseModule().(*Module).descendants, moduleId)

	//清理被删除的Module
	pModule.self = nil
	pModule.parent = nil
	pModule.childs = nil
	pModule.ancestor = nil
	pModule.descendants = nil
}

func (m *Module) SetModuleId(moduleId uint32) bool {
	if m.moduleId > 0 {
		return false
	}

	m.moduleId = moduleId
	return true
}

func (m *Module) GetModuleId() uint32 {
	return m.moduleId
}

func (m *Module) GetModuleName() string {
	return m.moduleName
}

func (m *Module) OnInit() error {
	return nil
}

func (m *Module) newModuleId() uint32 {
	m.ancestor.getBaseModule().(*Module).seedModuleId += 1
	return m.ancestor.getBaseModule().(*Module).seedModuleId
}

func (m *Module) GetAncestor() IModule {
	return m.ancestor
}

func (m *Module) GetModule(moduleId uint32) IModule {
	iModule, ok := m.GetAncestor().getBaseModule().(*Module).descendants[moduleId]
	if !ok {
		return nil
	}
	return iModule
}

func (m *Module) getBaseModule() IModule {
	return m
}

func (m *Module) GetParent() IModule {
	return m.parent
}

func (m *Module) OnRelease() {
}
