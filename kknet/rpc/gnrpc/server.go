package gnrpc

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
	"github.com/vvisun/kkdg/utils/kkcodec"
)

// Server is a unary RPC server implemented on top of kktcp(gnet).
type Server struct {
	addr string
	opts kknet.Options

	codec  *codec
	router *router
	tcp    *kktcp.Server

	serverInterceptors []UnaryServerInterceptor

	onewayQueue   chan onewayTask
	onewayDone    chan struct{}
	onewayStarted atomic.Bool
	onewayEnq     atomic.Int64
	onewayDrop    atomic.Int64
	onewayProc    atomic.Int64

	onewayPolicy       OnewayDropPolicy
	onewayBlockTimeout time.Duration
}

// NewServer creates a new RPC server.
// By default it uses msgpack for internal frame encoding.
func NewServer(addr string, opts ...kknet.Option) *Server {
	cfg := kknet.ApplyOptions(opts...)
	c, _ := newCodec(kkcodec.CodecTypeMsgpack)
	s := &Server{
		addr:   addr,
		opts:   cfg,
		codec:  c,
		router: newRouter(),
	}
	s.tcp = kktcp.NewServer(addr, &serverHandler{svr: s}, opts...)
	return s
}

type OnewayStats struct {
	Enqueued  int64
	Dropped   int64
	Processed int64
}

func (s *Server) OnewayStats() OnewayStats {
	return OnewayStats{
		Enqueued:  s.onewayEnq.Load(),
		Dropped:   s.onewayDrop.Load(),
		Processed: s.onewayProc.Load(),
	}
}

// OnewayDropPolicy defines behavior when async oneway queue is full.
type OnewayDropPolicy int

const (
	// OnewayDropNewest drops the incoming request when queue is full.
	OnewayDropNewest OnewayDropPolicy = iota
	// OnewayDropOldest drops one queued request to make room.
	OnewayDropOldest
	// OnewayBlock waits up to a timeout to enqueue.
	OnewayBlock
)

type OnewayOption func(*Server)

func WithOnewayPolicy(p OnewayDropPolicy) OnewayOption {
	return func(s *Server) { s.onewayPolicy = p }
}

// WithOnewayBlockTimeout sets max wait time for OnewayBlock policy.
// If <=0, OnewayBlock behaves like OnewayDropNewest.
func WithOnewayBlockTimeout(d time.Duration) OnewayOption {
	return func(s *Server) { s.onewayBlockTimeout = d }
}

// EnableOnewayAsync enables async processing for FrameTypeOneway.
// workers: number of worker goroutines (>=1). queueSize: buffered tasks (>=0).
//
// Default policy is OnewayDropNewest.
func (s *Server) EnableOnewayAsync(workers int, queueSize int) {
	s.EnableOnewayAsyncWithOptions(workers, queueSize)
}

// EnableOnewayAsyncWithOptions is like EnableOnewayAsync, but allows custom policy.
func (s *Server) EnableOnewayAsyncWithOptions(workers int, queueSize int, opts ...OnewayOption) {
	if workers <= 0 {
		workers = 1
	}
	if queueSize < 0 {
		queueSize = 0
	}
	for _, opt := range opts {
		if opt != nil {
			opt(s)
		}
	}
	if s.onewayPolicy < OnewayDropNewest || s.onewayPolicy > OnewayBlock {
		s.onewayPolicy = OnewayDropNewest
	}

	s.onewayQueue = make(chan onewayTask, queueSize)
	s.onewayDone = make(chan struct{})
	if s.onewayStarted.Swap(true) {
		return
	}
	for i := 0; i < workers; i++ {
		go s.onewayWorker()
	}
}

func (s *Server) onewayWorker() {
	q := s.onewayQueue
	done := s.onewayDone
	for {
		select {
		case <-done:
			return
		case task := <-q:
			task.run()
			s.onewayProc.Add(1)
		}
	}
}

// SetFrameCodec sets the codec used for encoding/decoding internal frames.
func (s *Server) SetFrameCodec(codecType uint8) error {
	c, err := newCodec(codecType)
	if err != nil {
		return err
	}
	s.codec = c
	return nil
}

// UseInterceptor adds server-side unary interceptors.
func (s *Server) UseInterceptor(interceptors ...UnaryServerInterceptor) {
	for _, it := range interceptors {
		if it != nil {
			s.serverInterceptors = append(s.serverInterceptors, it)
		}
	}
}

// Register registers a unary handler for method.
func (s *Server) Register(method string, h Handler) {
	s.router.Register(method, h)
}

func (s *Server) Start() error { return s.tcp.Start() }
func (s *Server) Stop() error {
	// stop tcp server first (no more traffic)
	err := s.tcp.Stop()

	// stop oneway workers (best effort)
	if s.onewayDone != nil {
		close(s.onewayDone)
		s.onewayDone = nil
	}
	s.onewayQueue = nil
	return err
}
func (s *Server) Addr() string { return s.tcp.Addr() }
func (s *Server) Stats() kknet.StatsSnapshot {
	return s.tcp.Stats()
}
func (s *Server) GetConnManager() kknet.IConnManager {
	return s.tcp.GetConnManager()
}

type serverHandler struct {
	svr *Server
}

func (h *serverHandler) OnConnect(_ kknet.IConn) {}

func (h *serverHandler) OnMessage(c kknet.IConn, data buffers.IBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}

	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	if err != nil {
		return
	}

	var fr Frame
	if err := h.svr.codec.Unmarshal(msgBytes, &fr); err != nil {
		return
	}
	if (fr.T != FrameTypeRequest && fr.T != FrameTypeOneway) || fr.M == "" {
		return
	}
	if fr.T == FrameTypeRequest && fr.ID == 0 {
		return
	}

	ctx, cancel := deadlineCtx(fr.DL)
	defer cancel()
	// Attach incoming metadata (headers) to context.
	if fr.H != nil {
		ctx = NewIncomingContext(ctx, MD(fr.H))
	}
	// Prepare response metadata container.
	ctx, meta := withServerMeta(ctx)
	// Apply server interceptor chain.
	base := func(ctx context.Context, req []byte) ([]byte, error) {
		return h.svr.router.Call(ctx, fr.M, req)
	}
	chained := chainServerInterceptors(h.svr.serverInterceptors, base, fr.M)
	if fr.T == FrameTypeOneway {
		// fire-and-forget: no response frame is sent.
		run := func() {
			_, callErr := chained(ctx, fr.P)
			if callErr != nil {
				h.svr.opts.Logger.Errorf("gnrpc oneway %s error: %v", fr.M, callErr)
			}
		}

		// async mode if enabled
		if q := h.svr.onewayQueue; q != nil {
			task := onewayTask{fn: run}
			switch h.svr.onewayPolicy {
			case OnewayDropOldest:
				// try enqueue; if full, drop one queued then retry once.
				select {
				case q <- task:
					h.svr.onewayEnq.Add(1)
				default:
					select {
					case <-q:
						h.svr.onewayDrop.Add(1)
					default:
					}
					select {
					case q <- task:
						h.svr.onewayEnq.Add(1)
					default:
						h.svr.onewayDrop.Add(1)
						h.svr.opts.Logger.Warnf("gnrpc oneway dropped (queue full): %s", fr.M)
					}
				}
				return
			case OnewayBlock:
				wait := h.svr.onewayBlockTimeout
				if wait <= 0 {
					// fallback
					select {
					case q <- task:
						h.svr.onewayEnq.Add(1)
					default:
						h.svr.onewayDrop.Add(1)
						h.svr.opts.Logger.Warnf("gnrpc oneway dropped (queue full): %s", fr.M)
					}
					return
				}
				// respect request deadline if earlier
				if dl, ok := ctx.Deadline(); ok {
					if until := time.Until(dl); until > 0 && until < wait {
						wait = until
					}
				}
				timer := time.NewTimer(wait)
				defer timer.Stop()
				select {
				case q <- task:
					h.svr.onewayEnq.Add(1)
				case <-timer.C:
					h.svr.onewayDrop.Add(1)
					h.svr.opts.Logger.Warnf("gnrpc oneway dropped (block timeout): %s", fr.M)
				}
				return
			default: // OnewayDropNewest
				select {
				case q <- task:
					h.svr.onewayEnq.Add(1)
				default:
					h.svr.onewayDrop.Add(1)
					h.svr.opts.Logger.Warnf("gnrpc oneway dropped (queue full): %s", fr.M)
				}
				return
			}
		}

		// inline mode
		run()
		return
	}
	respPayload, callErr := chained(ctx, fr.P)

	resp := Frame{
		T:  FrameTypeResponse,
		ID: fr.ID,
		P:  respPayload,
		RH: meta.headers,
		RT: meta.trailers,
	}
	if callErr != nil {
		resp.Code = int32(CodeOf(callErr))
		resp.Err = MsgOf(callErr)
	}

	b, err := h.svr.codec.Marshal(&resp)
	if err != nil {
		return
	}
	bb, err := kkpacket.DefaultStreamPacket().Pack(b)
	if err != nil {
		return
	}
	if err := c.SendBuffer(bb); err != nil {
		kkbuffer.Put(bb)
	}
}

func (h *serverHandler) OnClose(_ kknet.IConn, _ error) {}

// For convenience: a helper to create context-aware handlers.
func UnaryHandler(fn func(ctx context.Context, req []byte) ([]byte, error)) Handler {
	return fn
}

type onewayTask struct {
	fn func()
}

func (t onewayTask) run() {
	if t.fn != nil {
		t.fn()
	}
}

