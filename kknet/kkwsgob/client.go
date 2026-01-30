package kkwsgob

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/panjf2000/gnet/v2"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Client represents a WebSocket client based on gnet + gobwas/ws.
type Client struct {
	urlStr  string
	handler kknet.IHandler
	opts    kknet.Options

	clientMu sync.Mutex
	client   *gnet.Client

	connMu sync.Mutex
	conn   kknet.IConn
	openCh chan struct{}

	connected    atomic.Bool
	started      atomic.Bool
	reconnecting atomic.Bool
	closing      atomic.Bool
	stopCh       chan struct{}

	handshakeMu  sync.Mutex
	handshakeErr error

	parsedURL *url.URL
	addr      string
	path      string

	stats kknet.Stats
}

var _ kknet.IClient = (*Client)(nil)

// NewClient creates a new WebSocket client.
func NewClient(url string, handler kknet.IHandler, opts ...kknet.Option) *Client {
	return &Client{
		urlStr:  url,
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
		stopCh:  make(chan struct{}),
	}
}

// Connect connects to the server and starts the gnet client engine.
func (c *Client) Connect() error {
	if c.connected.Swap(true) {
		return nil
	}
	c.closing.Store(false)
	select {
	case <-c.stopCh:
		c.stopCh = make(chan struct{})
	default:
	}

	u, err := url.ParseRequestURI(c.urlStr)
	if err != nil {
		c.connected.Store(false)
		return err
	}
	if u.Scheme == "" || u.Host == "" {
		c.connected.Store(false)
		return errors.New("invalid websocket url")
	}
	if u.Scheme == "wss" || c.opts.TLSConfig != nil {
		c.connected.Store(false)
		return errors.New("kkwsgob does not support TLS, use kkwstls")
	}
	addr := u.Host
	if !strings.Contains(addr, ":") {
		addr = addr + ":80"
	}
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	c.parsedURL = u
	c.addr = addr
	c.path = path

	if !c.started.Swap(true) {
		ev := &wsClientEventHandler{client: c}
		cli, err := gnet.NewClient(ev,
			gnet.WithReadBufferCap(c.opts.ReadBufferSize),
			gnet.WithWriteBufferCap(c.opts.WriteBufferSize),
		)
		if err != nil {
			c.started.Store(false)
			c.connected.Store(false)
			return err
		}
		if err := cli.Start(); err != nil {
			c.started.Store(false)
			c.connected.Store(false)
			return err
		}
		c.clientMu.Lock()
		c.client = cli
		c.clientMu.Unlock()
	}

	openCh := make(chan struct{})
	c.connMu.Lock()
	c.openCh = openCh
	c.connMu.Unlock()

	c.clientMu.Lock()
	cli := c.client
	c.clientMu.Unlock()
	if cli == nil {
		c.connected.Store(false)
		return kkerrors.ErrClientNotConnected
	}

	if _, err := cli.Dial("tcp", c.addr); err != nil {
		c.connected.Store(false)
		return err
	}

	<-openCh
	c.handshakeMu.Lock()
	hsErr := c.handshakeErr
	c.handshakeMu.Unlock()
	if hsErr != nil {
		c.connected.Store(false)
		return hsErr
	}
	return nil
}

// SendBuffer sends a buffer to the server.
func (c *Client) SendBuffer(buffer buffers.IBuffer) error {
	if buffer == nil {
		return kkerrors.ErrInvalidPacket
	}
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrClientNotConnected
	}
	return conn.SendBuffer(buffer)
}

// Close closes the client connection.
func (c *Client) Close() error {
	c.connMu.Lock()
	conn := c.conn
	c.conn = nil
	c.connMu.Unlock()
	if conn == nil {
		return kkerrors.ErrClientNotConnected
	}
	c.closing.Store(true)
	c.connected.Store(false)
	c.reconnecting.Store(false)
	select {
	case <-c.stopCh:
	default:
		close(c.stopCh)
	}
	_ = conn.Close()
	c.clientMu.Lock()
	cli := c.client
	c.client = nil
	c.clientMu.Unlock()
	if cli != nil {
		_ = cli.Stop()
	}
	return nil
}

// Addr returns the remote address.
func (c *Client) Addr() string {
	return c.urlStr
}

// Stats returns a snapshot of client statistics.
func (c *Client) Stats() kknet.StatsSnapshot {
	return c.stats.Snapshot()
}

func (c *Client) SetContext(ctx context.Context) {
	c.connMu.Lock()
	conn := c.conn
	c.connMu.Unlock()
	if conn != nil {
		conn.SetContext(ctx)
	}
}

func (c *Client) startReconnect() {
	if c.reconnecting.Swap(true) {
		return
	}
	go c.reconnectLoop()
}

func (c *Client) reconnectLoop() {
	interval := c.opts.TcpClientReconnectInterval
	if interval <= 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	maxRetries := c.opts.TcpClientReconnectMaxRetries
	cb := c.opts.TcpClientReconnectCallback
	attempts := 0
	for {
		if c.closing.Load() {
			c.reconnecting.Store(false)
			return
		}
		if maxRetries > 0 && attempts >= maxRetries {
			if cb != nil {
				cb(attempts, errors.New("reconnect attempts exceeded"))
			}
			c.opts.Logger.Warnf("kkwsgob client reconnect exceeded after %d attempts", attempts)
			c.reconnecting.Store(false)
			return
		}
		attempts++
		c.opts.Logger.Debugf("kkwsgob client reconnect attempt %d", attempts)

		openCh := make(chan struct{})
		c.connMu.Lock()
		c.openCh = openCh
		c.connMu.Unlock()

		c.clientMu.Lock()
		cli := c.client
		c.clientMu.Unlock()
		if cli == nil {
			c.reconnecting.Store(false)
			return
		}

		if _, err := cli.Dial("tcp", c.addr); err == nil {
			select {
			case <-openCh:
				c.handshakeMu.Lock()
				hsErr := c.handshakeErr
				c.handshakeMu.Unlock()
				if hsErr == nil {
					if cb != nil {
						cb(attempts, nil)
					}
					c.opts.Logger.Infof("kkwsgob client reconnected after %d attempts", attempts)
					c.reconnecting.Store(false)
					return
				}
				if cb != nil {
					cb(attempts, hsErr)
				}
				c.opts.Logger.Warnf("kkwsgob client reconnect handshake failed: %v", hsErr)
			case <-c.stopCh:
				c.reconnecting.Store(false)
				return
			}
		} else {
			if cb != nil {
				cb(attempts, err)
			}
			c.opts.Logger.Warnf("kkwsgob client reconnect attempt %d failed: %v", attempts, err)
		}

		select {
		case <-time.After(interval):
		case <-c.stopCh:
			c.reconnecting.Store(false)
			return
		}
	}
}

type wsClientEventHandler struct {
	*gnet.BuiltinEventEngine
	client *Client
}

func (h *wsClientEventHandler) OnOpen(c gnet.Conn) (out []byte, action gnet.Action) {
	req, expectedAccept, err := buildHandshakeRequest(h.client.parsedURL, h.client.path)
	if err != nil {
		h.setHandshakeErr(err)
		h.signalOpen()
		return nil, gnet.Close
	}

	cc := newWSConn(c, h.client.opts, &h.client.stats, ws.StateClientSide)
	cc.expectedAccept = expectedAccept
	c.SetContext(cc)

	if err := asyncWriteBytes(c, req); err != nil {
		h.setHandshakeErr(err)
		h.signalOpen()
		return nil, gnet.Close
	}
	return nil, gnet.None
}

func (h *wsClientEventHandler) OnClose(c gnet.Conn, err error) (action gnet.Action) {
	cc, ok := c.Context().(*wsConn)
	if ok && cc.fragBuf != nil {
		kkbuffer.Put(cc.fragBuf)
		cc.fragBuf = nil
	}

	if ok && cc != nil && cc.upgraded {
		h.client.stats.OnClose()
		if err != nil && !isExpectedCloseErr(err) {
			h.client.stats.AddError()
		}
	}
	if ok && cc != nil && h.client.handler != nil && cc.upgraded {
		kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "kkwsgob client OnClose", func() {
			h.client.handler.OnClose(cc, errOrDefault(err, cc.getCloseErr()))
		})
	}

	h.client.connMu.Lock()
	h.client.conn = nil
	h.client.connMu.Unlock()
	h.client.connected.Store(false)

	if !h.client.closing.Load() && h.client.opts.TcpClientNeedReconnect {
		h.client.startReconnect()
	}
	return gnet.None
}

func (h *wsClientEventHandler) OnTraffic(c gnet.Conn) (action gnet.Action) {
	cc, ok := c.Context().(*wsConn)
	if !ok {
		return gnet.Close
	}

	if !cc.upgraded {
		upgraded, err := h.tryUpgrade(c, cc)
		if err != nil {
			cc.setCloseErr(err)
			h.setHandshakeErr(err)
			h.signalOpen()
			return gnet.Close
		}
		if !upgraded {
			return gnet.None
		}
		h.setHandshakeErr(nil)
		h.signalOpen()
	}

	for {
		hdr, payload, ok, err := cc.nextFrame()
		if err != nil {
			cc.setCloseErr(err)
			h.sendCloseOnError(cc, err)
			return gnet.Close
		}
		if !ok {
			return gnet.None
		}

		if hdr.OpCode.IsControl() {
			if h.handleControl(cc, hdr, payload) {
				return gnet.Close
			}
			continue
		}

		if err := h.handleData(cc, hdr, payload); err != nil {
			cc.setCloseErr(err)
			h.sendCloseOnError(cc, err)
			return gnet.Close
		}
	}
}

func (h *wsClientEventHandler) tryUpgrade(c gnet.Conn, cc *wsConn) (bool, error) {
	n := c.InboundBuffered()
	if n <= 0 {
		return false, nil
	}
	buf, err := c.Peek(n)
	if err != nil {
		return true, err
	}
	headerEnd := bytes.Index(buf, []byte("\r\n\r\n"))
	if headerEnd < 0 {
		return false, nil
	}

	resp, consumed, err := readHTTPResponse(buf)
	if err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
			return false, nil
		}
		return true, err
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		return true, ws.ErrHandshakeBadStatus
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") {
		return true, ws.ErrHandshakeBadUpgrade
	}
	if !hasToken(resp.Header.Get("Connection"), "upgrade") {
		return true, ws.ErrHandshakeBadConnection
	}
	if accept := resp.Header.Get("Sec-WebSocket-Accept"); accept != cc.expectedAccept {
		return true, ws.ErrHandshakeBadSecAccept
	}

	_, _ = c.Discard(consumed)
	cc.upgraded = true
	h.client.stats.OnConnect()

	h.client.connMu.Lock()
	h.client.conn = cc
	h.client.connMu.Unlock()

	if h.client.handler != nil {
		kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "kkwsgob client OnConnect", func() {
			h.client.handler.OnConnect(cc)
		})
	}
	return true, nil
}

func (h *wsClientEventHandler) handleData(cc *wsConn, hdr ws.Header, payload []byte) error {
	switch hdr.OpCode {
	case ws.OpText, ws.OpBinary:
		if !hdr.Fin {
			cc.state = cc.state.Set(ws.StateFragmented)
			return cc.appendFragment(hdr.OpCode, payload)
		}
		dataCpy := kkbuffer.GetWithCapacity(len(payload))
		dataCpy.B = dataCpy.B[:len(payload)]
		copy(dataCpy.B, payload)
		h.client.stats.AddRecv(len(payload))
		if h.client.handler != nil {
			kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "kkwsgob client OnMessage", func() {
				h.client.handler.OnMessage(cc, dataCpy)
			})
		}
		kkbuffer.Put(dataCpy)
		return nil
	case ws.OpContinuation:
		if cc.fragBuf == nil {
			return ws.ErrProtocolContinuationUnexpected
		}
		if err := cc.appendFragment(cc.fragOp, payload); err != nil {
			return err
		}
		if hdr.Fin {
			dataCpy := cc.takeFragment()
			cc.state = cc.state.Clear(ws.StateFragmented)
			h.client.stats.AddRecv(len(dataCpy.B))
			if h.client.handler != nil {
				kknet.SafeHandlerCall(h.client.opts.Logger, &h.client.stats, "kkwsgob client OnMessage", func() {
					h.client.handler.OnMessage(cc, dataCpy)
				})
			}
			kkbuffer.Put(dataCpy)
		}
		return nil
	default:
		return ws.ErrProtocolOpCodeReserved
	}
}

func (h *wsClientEventHandler) handleControl(cc *wsConn, hdr ws.Header, payload []byte) bool {
	switch hdr.OpCode {
	case ws.OpPing:
		cc.writeControl(ws.OpPong, payload)
		return false
	case ws.OpPong:
		return false
	case ws.OpClose:
		cc.writeControl(ws.OpClose, payload)
		return true
	default:
		return true
	}
}

func (h *wsClientEventHandler) sendCloseOnError(cc *wsConn, err error) {
	code := ws.StatusProtocolError
	if errors.Is(err, kkerrors.ErrMaxMessageSize) {
		code = ws.StatusMessageTooBig
	}
	payload := ws.NewCloseFrameBody(code, "")
	cc.writeControl(ws.OpClose, payload)
}

func (h *wsClientEventHandler) setHandshakeErr(err error) {
	h.client.handshakeMu.Lock()
	h.client.handshakeErr = err
	h.client.handshakeMu.Unlock()
}

func (h *wsClientEventHandler) signalOpen() {
	h.client.connMu.Lock()
	openCh := h.client.openCh
	h.client.openCh = nil
	h.client.connMu.Unlock()
	if openCh != nil {
		close(openCh)
	}
}

func buildHandshakeRequest(u *url.URL, path string) ([]byte, string, error) {
	nonce, err := newNonce()
	if err != nil {
		return nil, "", err
	}
	accept := computeAcceptKey(nonce)
	if path == "" {
		path = "/"
	}
	var b strings.Builder
	b.Grow(256)
	b.WriteString("GET ")
	b.WriteString(path)
	b.WriteString(" HTTP/1.1\r\n")
	b.WriteString("Host: ")
	b.WriteString(u.Host)
	b.WriteString("\r\n")
	b.WriteString("Upgrade: websocket\r\n")
	b.WriteString("Connection: Upgrade\r\n")
	b.WriteString("Sec-WebSocket-Key: ")
	b.WriteString(nonce)
	b.WriteString("\r\n")
	b.WriteString("Sec-WebSocket-Version: 13\r\n")
	b.WriteString("\r\n")
	return []byte(b.String()), accept, nil
}

func newNonce() (string, error) {
	key := make([]byte, 16)
	if _, err := rand.Read(key); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(key), nil
}

func computeAcceptKey(nonce string) string {
	const magic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
	sum := sha1.Sum([]byte(nonce + magic))
	return base64.StdEncoding.EncodeToString(sum[:])
}

func asyncWriteBytes(c gnet.Conn, payload []byte) error {
	if len(payload) == 0 {
		return nil
	}
	bb := kkbuffer.GetWithCapacity(len(payload))
	bb.B = bb.B[:len(payload)]
	copy(bb.B, payload)
	// TODO: 发送失败应该入队，下次优先从队列中取数据发送。
	if err := c.AsyncWrite(bb.B, func(_ gnet.Conn, err error) error {
		kkbuffer.Put(bb)
		if err != nil {
			// 发送失败
			return nil
		}
		return nil
	}); err != nil {
		// 入队失败
		kkbuffer.Put(bb)
		return err
	}
	// 入队成功立即返回，注意这里只是入队，并非真正的发送数据
	return nil
}

func readHTTPResponse(buf []byte) (*http.Response, int, error) {
	reader := bytes.NewReader(buf)
	br := bufio.NewReader(reader)
	resp, err := http.ReadResponse(br, &http.Request{Method: http.MethodGet})
	if err != nil {
		return nil, 0, err
	}
	remaining := reader.Len() + br.Buffered()
	return resp, len(buf) - remaining, nil
}

func hasToken(header, token string) bool {
	header = strings.ToLower(header)
	token = strings.ToLower(token)
	for _, part := range strings.Split(header, ",") {
		if strings.TrimSpace(part) == token {
			return true
		}
	}
	return false
}
