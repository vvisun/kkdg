package kkpool

import (
	"reflect"
	"sync"
)

var (
	factoryMap   = make(map[reflect.Type]*SfxPool[interface{}])
	factoryMutex sync.Mutex
)

// GetFactoryByType 获取指定类型的工厂
// 注意: 传入的类型必须是指针类型
func GetFactoryByType(tp reflect.Type) *SfxPool[interface{}] {
	factoryMutex.Lock()
	defer factoryMutex.Unlock()
	factory, ok := factoryMap[tp]
	if !ok {
		factory = NewSfxPool(func() interface{} {
			return reflect.New(tp.Elem()).Interface()
		})
		factoryMap[tp] = factory
	}
	return factory
}

// GetFactory 获取指定类型的工厂
func GetFactory[T any]() *SfxPool[interface{}] {
	var v T
	tp := reflect.TypeOf(&v)
	factory := GetFactoryByType(tp)
	return factory
}

func NewObject[T any]() *T {
	obj := GetFactory[T]().Get().(*T)
	if obj == nil {
		return new(T)
	}
	return obj
}

func FreeObject(obj any) {
	if obj == nil {
		return
	}
	factory := GetFactoryByType(reflect.TypeOf(obj))
	if factory == nil {
		return
	}
	factory.Put(obj)
}
