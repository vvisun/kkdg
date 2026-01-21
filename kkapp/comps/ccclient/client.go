package ccclient

import (
	"errors"
	"sync"
	"time"

	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/kknet/kkws"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/kklog"
)

// 客户端。模拟用，测试用
type ClientComponent struct {
	component.Component
	opt Option

	client   clientSender
	stopOnce sync.Once
	stopCh   chan struct{}
	doneCh   chan struct{}
}

type clientSender interface {
	Connect() error
	Close() error
	Send(data []byte) error
}

func (slf *ClientComponent) GetID() string {
	return "client"
}

// NewClientComponent creates a new client component.
func NewClientComponent(opt Option) *ClientComponent {
	return &ClientComponent{
		opt: opt,
	}
}

var _ component.IComponent = (*ClientComponent)(nil)

func (slf *ClientComponent) Init() error {
	slf.stopCh = make(chan struct{})
	slf.doneCh = make(chan struct{})
	return nil
}

func (slf *ClientComponent) Start() error {
	handler := &clientHandler{client: slf}

	var client clientSender
	if slf.opt.WSURL != "" {
		client = kkws.NewClient(
			slf.opt.WSURL,
			handler,
			kknet.WithLogger(kklog.Stdout()),
		)
	} else if slf.opt.TCPAddr != "" {
		client = kktcp.NewClient(
			slf.opt.TCPAddr,
			handler,
			kknet.WithLogger(kklog.Stdout()),
			kknet.WithStreamPacket(kkpacket.NewLengthFieldStreamPacket(nil)),
		)
	} else {
		return errors.New("ccclient: TCPAddr or WSURL must be set")
	}

	slf.client = client
	if err := slf.client.Connect(); err != nil {
		return err
	}

	payload := slf.opt.Payload
	if len(payload) == 0 {
		payload = nil
	}
	if len(payload) > 0 {
		go slf.sendLoop(payload)
	} else {
		close(slf.doneCh)
	}

	return nil
}

func (slf *ClientComponent) Stop() error {
	slf.stopOnce.Do(func() {
		close(slf.stopCh)
	})
	if slf.doneCh != nil {
		<-slf.doneCh
	}
	if slf.client != nil {
		if err := slf.client.Close(); err != nil {
			kklog.Errorf("[ccclient] close error: %v", err)
		}
	}
	return nil
}

func (slf *ClientComponent) sendLoop(payload []byte) {
	defer close(slf.doneCh)

	interval := slf.opt.SendInterval
	if interval <= 0 {
		interval = 0
	}
	count := slf.opt.SendCount
	if count == 0 {
		count = 1
	}

	sendOnce := func() bool {
		if slf.client == nil {
			return false
		}
		if err := slf.client.Send(payload); err != nil {
			kklog.Errorf("[ccclient] send error: %v", err)
			return false
		}
		return true
	}

	if interval <= 0 {
		for i := 0; i < count; i++ {
			select {
			case <-slf.stopCh:
				return
			default:
				if !sendOnce() {
					return
				}
			}
		}
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for sent := 0; count < 0 || sent < count; sent++ {
		select {
		case <-slf.stopCh:
			return
		case <-ticker.C:
			if !sendOnce() {
				return
			}
		}
	}
}

type clientHandler struct {
	client *ClientComponent
}

func (h *clientHandler) OnConnect(c kknet.IConn) {
	kklog.Infof("[ccclient] connected: remoteAddr=%s", c.RemoteAddr())
}

func (h *clientHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	kklog.Infof("[ccclient] recv: size=%d", len(data.Bytes()))
}

func (h *clientHandler) OnClose(c kknet.IConn, err error) {
	kklog.Infof("[ccclient] closed: remoteAddr=%s err=%v", c.RemoteAddr(), err)
}
