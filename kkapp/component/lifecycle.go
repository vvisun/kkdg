package component

import (
	"sync/atomic"

	"github.com/vvisun/kkdg/kkerrors"
)

// Lifecycle implements a default component for Component.
type Lifecycle struct {
	state ComponentState
}

var _ IComponentLifecycle = (*Lifecycle)(nil)

// Init was called to initialize the component.
func (slf *Lifecycle) Init() error {
	if atomic.LoadInt64(&slf.state) != ComponentStateNone {
		return kkerrors.ErrComponentNotInitialized
	}
	atomic.StoreInt64(&slf.state, ComponentStateInit)
	return nil
}

// AfterInit was called after the component is initialized.
func (slf *Lifecycle) AfterInit() error {
	if atomic.LoadInt64(&slf.state) != ComponentStateInit {
		return kkerrors.ErrComponentNotInitialized
	}
	atomic.StoreInt64(&slf.state, ComponentStateAfterInit)
	return nil
}

// BeforeShutdown was called before the component to shutdown.
func (slf *Lifecycle) BeforeShutdown() error {
	if atomic.LoadInt64(&slf.state) != ComponentStateAfterInit {
		return kkerrors.ErrComponentNotInitialized
	}
	atomic.StoreInt64(&slf.state, ComponentStateBeforeShutdown)
	return nil
}

// Shutdown was called to shutdown the component.
func (slf *Lifecycle) Shutdown() error {
	if atomic.LoadInt64(&slf.state) != ComponentStateBeforeShutdown {
		return kkerrors.ErrComponentNotShutdown
	}
	atomic.StoreInt64(&slf.state, ComponentStateShutdown)
	return nil
}

// GetState was called to get the state of the component.
func (slf *Lifecycle) GetState() ComponentState {
	return atomic.LoadInt64(&slf.state)
}
