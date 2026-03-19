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
		if !IsValidFaultAction(EFaultAction(action)) {
			kklog.Errorf("[faultreport] rule compName %s action %d is invalid", compName, action)
			continue
		}
		slf.rules[compName] = EFaultAction(action)
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
