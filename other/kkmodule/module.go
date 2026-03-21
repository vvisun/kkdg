package kkmodule

import (
	"fmt"
	"reflect"
	"slices"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
)

type IModule interface {
	GetModuleId() uint32
	GetModuleName() string
	FullName() string

	AddModule(module IModule) (uint32, error)
	ReleaseModule(moduleId uint32)

	GetModule(moduleId uint32) IModule
	GetAncestor() IModule
	GetParent() IModule

	OnInit() error
	OnStop()

	getBaseModule() IModule
}

var moduleIdCounter uint32 = 0

func autoModuleId() uint32 {
	return atomic.AddUint32(&moduleIdCounter, 1)
}

type Module struct {
	moduleId   uint32 //模块Id
	moduleName string //模块名称

	parent      IModule            //父亲
	self        IModule            //自己
	childs      []IModule          //孩子们
	ancestor    IModule            //始祖
	descendants map[uint32]IModule //始祖的后裔们
}

var _ IModule = (*Module)(nil)

func (m *Module) getBaseModule() IModule {
	return m
}

func (m *Module) AddModule(module IModule) (uint32, error) {
	pAddModule := module.getBaseModule().(*Module)
	if pAddModule.GetModuleId() == 0 {
		pAddModule.moduleId = autoModuleId()
	}

	if module.GetModuleId() == m.GetModuleId() {
		return 0, fmt.Errorf("module id %d already exists", module.GetModuleId())
	}
	_, ok := m.ancestor.getBaseModule().(*Module).descendants[module.GetModuleId()]
	if ok {
		return 0, fmt.Errorf("module id %d already exists", module.GetModuleId())
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
		kklog.Errorf("[kkmodule] module %s OnInit error: %v", module.GetModuleName(), err)
		return 0, err
	}

	kklog.Debugf("[kkmodule] add module %s completed", module.GetModuleName())
	return module.GetModuleId(), nil
}

func (m *Module) ReleaseModule(moduleId uint32) {
	curMod := m.GetModule(moduleId)
	if curMod == nil {
		kklog.Debugf("[kkmodule] release module %d not found", moduleId)
		return
	}
	pModule := curMod.getBaseModule().(*Module)
	pModule.self.OnStop()
	kklog.Debugf("[kkmodule] release module %s", pModule.GetModuleName())

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

func (m *Module) GetModule(moduleId uint32) IModule {
	iModule, ok := m.GetAncestor().getBaseModule().(*Module).descendants[moduleId]
	if !ok {
		return nil
	}
	return iModule
}

func (m *Module) GetModuleId() uint32 {
	return m.moduleId
}

func (m *Module) GetModuleName() string {
	return m.moduleName
}

func (m *Module) FullName() string {
	name := m.moduleName
	parent := m.parent
	for parent != nil {
		name = parent.GetModuleName() + "." + name
		parent = parent.GetParent()
	}
	return name
}

func (m *Module) GetAncestor() IModule {
	return m.ancestor
}

func (m *Module) GetParent() IModule {
	return m.parent
}

func (m *Module) OnInit() error {
	kklog.Debugf("[kkmodule] module %s on init", m.GetModuleName())
	return nil
}

func (m *Module) OnStop() {
	kklog.Debugf("[kkmodule] module %s on release", m.GetModuleName())
}
