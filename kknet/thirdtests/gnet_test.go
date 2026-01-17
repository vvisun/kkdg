package thirdtests

import (
	"sync"
	"testing"
	"time"

	"github.com/panjf2000/gnet/v2"
)

type testHandler struct {
}

func (ts *testHandler) OnBoot(eng gnet.Engine) (action gnet.Action) {
	return gnet.None
}

func (ts *testHandler) OnShutdown(eng gnet.Engine) {

}

func (ts *testHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	return nil, gnet.None
}

func (ts *testHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	return gnet.None
}

func (ts *testHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	return gnet.None
}

func (ts *testHandler) OnTick() (delay time.Duration, action gnet.Action) {
	return 0, gnet.None
}

func (ts *testHandler) OnError(c gnet.Conn, err error) (action gnet.Action) {
	return gnet.None
}

func TestGnet(t *testing.T) {

	wg := sync.WaitGroup{}
	wg.Add(1)

	go func() {
		evtHandler := &testHandler{}
		gnet.Run(evtHandler, "tcp://0.0.0.0:9701", gnet.WithMulticore(true))
		wg.Done()
	}()

	wg.Wait()
}
