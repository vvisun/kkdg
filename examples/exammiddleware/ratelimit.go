package exammiddleware

import (
	"fmt"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
)

// RateLimitMiddleware limits message processing rate.
func RateLimitMiddleware(maxPerSecond int) kknet.Middleware {
	lastTime := time.Now()
	count := 0
	return func(next kknet.IHandler) kknet.IHandler {
		return &rateLimitHandler{
			next:         next,
			maxPerSecond: maxPerSecond,
			lastTime:     &lastTime,
			count:        &count,
		}
	}
}

type rateLimitHandler struct {
	next         kknet.IHandler
	maxPerSecond int
	lastTime     *time.Time
	count        *int
}

func (h *rateLimitHandler) OnConnect(c kknet.IConn) {
	if h.next != nil {
		h.next.OnConnect(c)
	}
}

func (h *rateLimitHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	now := time.Now()
	if now.Sub(*h.lastTime) >= time.Second {
		*h.lastTime = now
		*h.count = 0
	}
	if *h.count >= h.maxPerSecond {
		fmt.Printf("[RATE LIMIT] Connection %d exceeded rate limit\n", c.ID())
		return
	}
	*h.count++
	if h.next != nil {
		h.next.OnMessage(c, data)
	}
}

func (h *rateLimitHandler) OnClose(c kknet.IConn, err error) {
	if h.next != nil {
		h.next.OnClose(c, err)
	}
}
