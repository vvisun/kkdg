package appclient

import (
	"errors"
	"sync"
	"time"

	"github.com/vvisun/kkdg/kkapp/component"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/kknet/kkws"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kklog"
)

// 客户端。模拟用，测试用
type AppClient struct {
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
	SendBuffer(buffer *kkbuffer.ByteBuffer) error
}

func (slf *AppClient) GetID() string {
	return "client"
}

// NewAppClient creates a new client component.
func NewAppClient(opt Option) *AppClient {
	return &AppClient{
		opt: opt,
	}
}

func (slf *AppClient) Init() error {
	slf.stopCh = make(chan struct{})
	slf.doneCh = make(chan struct{})
	return nil
}

func (slf *AppClient) Start() error {
	handler := &clientHandler{client: slf}

	var client clientSender
	opts := kknet.ApplyOptions(
		kknet.WithLogger(kklog.Stdout()),
	)
	if slf.opt.WSURL != "" {
		client = kkws.NewClient(slf.opt.WSURL, handler, opts)
	} else if slf.opt.TCPAddr != "" {
		opts := kknet.ApplyOptions(
			kknet.WithLogger(kklog.Stdout()),
		)
		client = kktcp.NewClient(slf.opt.TCPAddr, handler, opts)
	} else {
		return errors.New("ccclient: TCPAddr or WSURL must be set")
	}

	slf.client = client
	if err := slf.client.Connect(); err != nil {
		return err
	}

	payload := []byte("hello from ccclient")
	if len(payload) > 0 {
		go slf.sendLoop(payload)
	} else {
		close(slf.doneCh)
	}

	return nil
}

func (slf *AppClient) Stop() error {
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

func (slf *AppClient) sendLoop(payload []byte) {
	defer close(slf.doneCh)

	interval := 5
	count := 20

	sendOnce := func() bool {
		if slf.client == nil {
			return false
		}
		bb, err := kkpacket.DefaultStreamPacket().Pack(payload)
		if err != nil {
			kklog.Errorf("[ccclient] pack error: %v", err)
			return false
		}
		if err := slf.client.SendBuffer(bb); err != nil {
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

	ticker := time.NewTicker(time.Duration(interval) * time.Millisecond)
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
	client *AppClient
}

func (h *clientHandler) OnConnect(c kknet.IConn) {
	kklog.Infof("[ccclient] connected: remoteAddr=%s", c.RemoteAddr())
}

func (h *clientHandler) OnMessage(c kknet.IConn, data *kkbuffer.ByteBuffer) {
	kklog.Infof("[ccclient] recv: size=%d", len(data.Bytes()))
}

func (h *clientHandler) OnClose(c kknet.IConn, err error) {
	kklog.Infof("[ccclient] closed: remoteAddr=%s err=%v", c.RemoteAddr(), err)
}
