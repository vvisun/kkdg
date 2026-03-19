package faultreport

import (
	"sync"
	"sync/atomic"

	"github.com/vvisun/kkdg/utils/kklog"
)

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

func (slf *RuleTable) FromMap(rules map[string]int) {
	if slf.locket.Load() {
		kklog.Errorf("[faultreport] rule table is locked")
		return
	}
	slf.mu.Lock()
	defer slf.mu.Unlock()
	for compName, action := range rules {
		if !IsValidFaultAction(EFaultAction(action)) {
			kklog.Errorf("[faultreport] rule compName %s action %d is invalid", compName, action)
			continue
		}
		slf.rules[compName] = EFaultAction(action)
	}
}

func (slf *RuleTable) AddRule(compName string, action EFaultAction) {
	if slf.locket.Load() {
		kklog.Errorf("[faultreport] rule table is locked")
		return
	}
	if compName == "" {
		kklog.Errorf("[faultreport] rule compName is empty")
		return
	}
	if !IsValidFaultAction(action) {
		kklog.Errorf("[faultreport] rule compName %s action %d is invalid", compName, action)
		return
	}
	slf.mu.Lock()
	defer slf.mu.Unlock()
	slf.rules[compName] = action
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
