package component

type Container struct {
	parent    IComponentContainer
	childlist []IComponentContainer
}

var _ IComponentContainer = (*Container)(nil)

func (slf *Container) HasChild(target IComponentContainer) bool {
	for _, child := range slf.childlist {
		if child == target {
			return true
		}
	}
	return false
}

func (slf *Container) AddChild(child IComponentContainer) {
	if slf.HasChild(child) {
		return
	}
	slf.childlist = append(slf.childlist, child)
	child.SetParent(slf)
}

func (slf *Container) RemoveChild(target IComponentContainer) {
	for i, child := range slf.childlist {
		if child == target {
			target.SetParent(nil)
			slf.childlist = append(slf.childlist[:i], slf.childlist[i+1:]...)
			break
		}
	}
}

func (slf *Container) SetParent(parent IComponentContainer) {
	if slf.parent != nil {
		slf.parent.RemoveChild(slf)
	}
	slf.parent = parent
}

func (slf *Container) GetParent() IComponentContainer {
	return slf.parent
}

func (slf *Container) GetChildrens() []IComponentContainer {
	return slf.childlist
}
