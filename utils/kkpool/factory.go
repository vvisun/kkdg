package kkpool

import (
	"reflect"
	"sync"
)

var (
	factoryMap   = make(map[reflect.Type]*SfxPool[interface{}])
	factoryMutex sync.Mutex
)

func GetFactory[T any]() *SfxPool[interface{}] {
	factoryMutex.Lock()
	defer factoryMutex.Unlock()
	var v T
	tp := reflect.TypeOf(&v)
	factory, ok := factoryMap[tp]
	if !ok {
		factory = NewSfxPool(func() interface{} {
			return new(T)
		})
		factoryMap[tp] = factory
	}
	return factory
}
