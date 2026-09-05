package faultreport

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
)

// 组件故障规则表
// 用于根据组件名称和故障类型，决定如何处理故障。未设置的组件默认停止应用。
// 因为某个组件故障后，大概率是这个组件存在bug，
// 为了防止故障扩散，停止应用并维护修复是最好的选择，否则大概率系统会带病运行，造成不可知的问题。
type RuleTable struct {
	rules  map[string]EFaultAction
	mu     sync.RWMutex
	locket atomic.Bool
}

func NewRuleTable() *RuleTable {
	return &RuleTable{
		rules: make(map[string]EFaultAction),
	}
}

// Lock 锁定规则表。
// 锁定后，只读。
func (slf *RuleTable) Lock() {
	slf.locket.Store(true)
}

func (slf *RuleTable) FromMap(rules map[string]EFaultAction) {
	if rules == nil {
		kklog.Errorf("[faultreport] rule map is nil")
		return
	}

	if slf.locket.Load() {
		kklog.Errorf("[faultreport] rule table is locked")
		return
	}

	slf.mu.Lock()
	defer slf.mu.Unlock()

	if slf.locket.Load() {
		kklog.Errorf("[faultreport] rule table is locked")
		return
	}

	for compName, action := range rules {
		if !IsValidFaultAction(action) {
			kklog.Errorf("[faultreport] rule compName %s action %d is invalid", compName, action)
			continue
		}
		slf.rules[compName] = action
	}
}

func (slf *RuleTable) GetRule(compName string) (EFaultAction, bool) {
	slf.mu.RLock()
	defer slf.mu.RUnlock()
	action, ok := slf.rules[compName]
	if !ok {
		return FaultActionStopApp, false
	}
	return action, ok
}
