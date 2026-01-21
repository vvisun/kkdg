package ccclient

import "github.com/vvisun/kkdg/kkapp/component"

//客户端。模拟用，测试用
type ClientComponent struct {
	component.Component
}

func (slf *ClientComponent) GetID() string {
	return "client"
}

var _ component.IComponent = (*ClientComponent)(nil)
