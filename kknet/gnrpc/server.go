package gnrpc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/kknet/kkpacket"
	"github.com/vvisun/kkdg/kknet/kktcp"
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

	methodStates sync.Map // method(string) -> *methodOnewayState
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
	h := &serverHandler{svr: s}
	// gnrpc relies on RawHandler delivery from ReadProcessor.
	reliesopts := kknet.ApplyOptions(
		kknet.WithRawHandler(h),
		kknet.WithBufferSizes(2*1024, 2*1024),
	)
	s.tcp = kktcp.NewServer(addr, h, reliesopts)
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

// SetMethodConfig sets per-method rate limit / breaker / in-flight limit.
// It applies to both request and oneway.
func (s *Server) SetMethodConfig(method string, cfg MethodConfig) {
	if method == "" {
		return
	}
	v, _ := s.methodStates.LoadOrStore(method, &methodState{})
	st := v.(*methodState)
	st.setConfig(cfg)
}

// GetMethodStats returns stats for a single method.
func (s *Server) GetMethodStats(method string) (MethodStats, bool) {
	v, ok := s.methodStates.Load(method)
	if !ok {
		return MethodStats{}, false
	}
	return v.(*methodState).snapshot(method), true
}

// GetAllMethodStats returns stats for all configured methods (and any methods seen).
func (s *Server) GetAllMethodStats() map[string]MethodStats {
	out := make(map[string]MethodStats)
	s.methodStates.Range(func(k, v any) bool {
		method := k.(string)
		out[method] = v.(*methodState).snapshot(method)
		return true
	})
	return out
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
func (s *Server) SetFrameCodec(codecType kkcodec.CodecType) error {
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

func (h *serverHandler) OnConnect(c kknet.IConn) {
	// attach per-connection peer state for server-initiated calls
	if c == nil {
		return
	}
	if ps, ok := getPeerState(c.Context()); ok && ps != nil {
		return
	}
	ps := newPeerState()
	c.SetContext(withPeerState(c.Context(), ps))
}

// OnRaw implements kknet.IRawHandler.
// Note: data is a framed packet: [length,message].
func (h *serverHandler) OnRaw(connId kknet.CONN_ID, data *kkbuffer.ByteBuffer) {
	if h == nil || h.svr == nil || data == nil || len(data.Bytes()) == 0 {
		return
	}
	cm := h.svr.GetConnManager()
	if cm == nil {
		return
	}
	c := cm.GetConn(connId)
	if c == nil {
		return
	}
	h.onPacket(c, data)
}

func rejectReasonToStatus(reason string) error {
	switch reason {
	case "rate", "inflight":
		return Status(CodeResourceExhausted, reason)
	case "breaker":
		return Status(CodeUnavailable, reason)
	default:
		return Status(CodeUnavailable, "rejected")
	}
}

func (h *serverHandler) getMethodState(method string) *methodState {
	if method == "" {
		return nil
	}
	if v, ok := h.svr.methodStates.Load(method); ok {
		return v.(*methodState)
	}
	v, _ := h.svr.methodStates.LoadOrStore(method, &methodState{})
	return v.(*methodState)
}

func (h *serverHandler) onPacket(c kknet.IConn, data *kkbuffer.ByteBuffer) {
	if data == nil || len(data.Bytes()) == 0 {
		return
	}

	msgBytes, err := kkpacket.DefaultStreamPacket().Unpack(data.Bytes())
	kkbuffer.Put(data)
	if err != nil {
		return
	}

	var fr Frame
	if err := h.svr.codec.Unmarshal(msgBytes, &fr); err != nil {
		return
	}
	// handle response for server-initiated invoke
	if fr.T == FrameTypeResponse && fr.ID != 0 {
		ps, ok := getPeerState(c.Context())
		if ok && ps != nil {
			ps.deliver(fr)
		}
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

	// NOTE: for any early-return after this point (incl. limiter rejects),
	// we must ensure method inFlight is released via ms.onProcessed/onDropped.
	// Apply server interceptor chain.
	base := func(ctx context.Context, req []byte) ([]byte, error) {
		return h.svr.router.Call(ctx, fr.M, req)
	}
	chained := chainServerInterceptors(h.svr.serverInterceptors, base, fr.M)

	// method-level control (applies to both request & oneway)
	ms := h.getMethodState(fr.M)
	acquired := false
	if ms != nil {
		if ok, reason := ms.tryAcquire(); !ok {
			// tryAcquire only increments inFlight on success.
			if fr.T == FrameTypeOneway {
				h.svr.onewayDrop.Add(1)
				if reason != "" {
					h.svr.opts.Logger.Warnf("gnrpc oneway rejected (%s): %s", reason, fr.M)
				}
				return
			}
			callErr := rejectReasonToStatus(reason)
			resp := Frame{
				T:    FrameTypeResponse,
				ID:   fr.ID,
				Code: int32(CodeOf(callErr)),
				Err:  MsgOf(callErr),
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
			return
		}
		acquired = true
	}
	if fr.T == FrameTypeOneway {
		// fire-and-forget: no response frame is sent.
		run := func() error {
			_, callErr := chained(ctx, fr.P)
			if callErr != nil {
				h.svr.opts.Logger.Errorf("gnrpc oneway %s error: %v", fr.M, callErr)
			}
			return callErr
		}

		// async mode if enabled
		if q := h.svr.onewayQueue; q != nil {
			task := onewayTask{method: fr.M, state: ms, fn: run}
			switch h.svr.onewayPolicy {
			case OnewayDropOldest:
				// try enqueue; if full, drop one queued then retry once.
				select {
				case q <- task:
					h.svr.onewayEnq.Add(1)
					ms.onEnqueued()
				default:
					select {
					case old := <-q:
						h.svr.onewayDrop.Add(1)
						if old.state != nil {
							old.state.onDropped()
						}
					default:
					}
					select {
					case q <- task:
						h.svr.onewayEnq.Add(1)
						ms.onEnqueued()
					default:
						h.svr.onewayDrop.Add(1)
						ms.onDropped()
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
					ms.onEnqueued()
				case <-timer.C:
					h.svr.onewayDrop.Add(1)
					ms.onDropped()
					h.svr.opts.Logger.Warnf("gnrpc oneway dropped (block timeout): %s", fr.M)
				}
				return
			default: // OnewayDropNewest
				select {
				case q <- task:
					h.svr.onewayEnq.Add(1)
					ms.onEnqueued()
				default:
					h.svr.onewayDrop.Add(1)
					ms.onDropped()
					h.svr.opts.Logger.Warnf("gnrpc oneway dropped (queue full): %s", fr.M)
				}
				return
			}
		}

		// inline mode
		callErr := run()
		if acquired && ms != nil {
			ms.onProcessed(callErr)
		}
		h.svr.onewayProc.Add(1)
		return
	}

	respPayload, callErr := chained(ctx, fr.P)
	if acquired && ms != nil {
		ms.onProcessed(callErr)
	}

	resp := Frame{
		T:  FrameTypeResponse,
		ID: fr.ID,
		P:  respPayload,
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

func (h *serverHandler) OnClose(c kknet.IConn, _ error) {
	if c == nil {
		return
	}
	if ps, ok := getPeerState(c.Context()); ok && ps != nil {
		ps.closeAll()
	}
}

// For convenience: a helper to create context-aware handlers.
func UnaryHandler(fn func(ctx context.Context, req []byte) ([]byte, error)) Handler {
	return fn
}

type onewayTask struct {
	method string
	state  *methodState
	fn     func() error
}

func (t onewayTask) run() {
	var err error
	if t.fn != nil {
		err = t.fn()
	}
	if t.state != nil {
		t.state.onProcessed(err)
	}
}
