package kkwstls

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gobwas/ws"
	"github.com/vvisun/kkdg/kkerrors"
	"github.com/vvisun/kkdg/kknet"
	"github.com/vvisun/kkdg/utils/buffers"
	"github.com/vvisun/kkdg/utils/buffers/kkbuffer"
)

// Client represents a TLS WebSocket client.
type Client struct {
	urlStr  string
	handler kknet.IHandler
	opts    kknet.Options

	connMu sync.Mutex
	conn   kknet.IConn

	connected    atomic.Bool
	reconnecting atomic.Bool
	closing      atomic.Bool
	stopCh       chan struct{}

	parsedURL *url.URL
	addr      string
	path      string

	stats kknet.Stats
}

var _ kknet.IClient = (*Client)(nil)

// NewClient creates a new TLS WebSocket client.
func NewClient(url string, handler kknet.IHandler, opts ...kknet.Option) *Client {
	return &Client{
		urlStr:  url,
		handler: handler,
		opts:    kknet.ApplyOptions(opts...),
		stopCh:  make(chan struct{}),
	}
}

// Connect connects to the server.
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
	if u.Scheme != "wss" && c.opts.TLSConfig == nil {
		c.connected.Store(false)
		return errors.New("kkwstls requires wss or TLSConfig")
	}
	addr := u.Host
	if !strings.Contains(addr, ":") {
		addr = addr + ":443"
	}
	path := u.RequestURI()
	if path == "" {
		path = "/"
	}
	c.parsedURL = u
	c.addr = addr
	c.path = path

	if err := c.connectTLS(); err != nil {
		c.connected.Store(false)
		return err
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
	interval := c.opts.ReconnectInterval
	if interval <= 500*time.Millisecond {
		interval = 500 * time.Millisecond
	}
	maxRetries := c.opts.ReconnectMaxRetries
	cb := c.opts.ReconnectCallback
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
			c.opts.Logger.Warnf("kkwstls client reconnect exceeded after %d attempts", attempts)
			c.reconnecting.Store(false)
			return
		}
		attempts++
		c.opts.Logger.Debugf("kkwstls client reconnect attempt %d", attempts)

		if err := c.connectTLS(); err == nil {
			if cb != nil {
				cb(attempts, nil)
			}
			c.opts.Logger.Infof("kkwstls client reconnected after %d attempts", attempts)
			c.reconnecting.Store(false)
			return
		} else if cb != nil {
			cb(attempts, err)
		}

		select {
		case <-time.After(interval):
		case <-c.stopCh:
			c.reconnecting.Store(false)
			return
		}
	}
}

func (c *Client) connectTLS() error {
	tlsConn, err := dialTLS(c.addr, c.opts.TLSConfig, c.parsedURL.Hostname())
	if err != nil {
		return err
	}

	req, expectedAccept, err := buildHandshakeRequest(c.parsedURL, c.path)
	if err != nil {
		_ = tlsConn.Close()
		return err
	}

	if c.opts.WsWriteTimeout > 0 {
		_ = tlsConn.SetWriteDeadline(time.Now().Add(c.opts.WsWriteTimeout))
	}
	if _, err := tlsConn.Write(req); err != nil {
		_ = tlsConn.Close()
		return err
	}

	reader := bufio.NewReader(tlsConn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
	if err != nil {
		_ = tlsConn.Close()
		return err
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		_ = tlsConn.Close()
		return ws.ErrHandshakeBadStatus
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") {
		_ = tlsConn.Close()
		return ws.ErrHandshakeBadUpgrade
	}
	if !hasToken(resp.Header.Get("Connection"), "upgrade") {
		_ = tlsConn.Close()
		return ws.ErrHandshakeBadConnection
	}
	if accept := resp.Header.Get("Sec-WebSocket-Accept"); accept != expectedAccept {
		_ = tlsConn.Close()
		return ws.ErrHandshakeBadSecAccept
	}

	cc := newNetWSConn(tlsConn, reader, c.opts, &c.stats, ws.StateClientSide)
	cc.upgraded = true

	c.connMu.Lock()
	c.conn = cc
	c.connMu.Unlock()

	c.stats.OnConnect()
	if c.handler != nil {
		kknet.SafeHandlerCall(c.opts.Logger, &c.stats, "kkwstls OnConnect", func() {
			c.handler.OnConnect(cc)
		})
	}

	go func() {
		err := cc.readLoop(func(msg buffers.IBuffer) {
			if c.handler != nil {
				kknet.SafeHandlerCall(c.opts.Logger, &c.stats, "kkwstls OnMessage", func() {
					c.handler.OnMessage(cc, msg)
				})
			}
			kkbuffer.Put(msg)
		})
		cc.setCloseErr(err)
		c.handleClose(cc, err)
	}()

	return nil
}

func (c *Client) handleClose(cc *netWSConn, err error) {
	c.stats.OnClose()
	if err != nil && !isExpectedCloseErr(err) {
		c.stats.AddError()
	}
	if c.handler != nil {
		kknet.SafeHandlerCall(c.opts.Logger, &c.stats, "kkwstls OnClose", func() {
			c.handler.OnClose(cc, errOrDefault(err, cc.getCloseErr()))
		})
	}
	c.connMu.Lock()
	c.conn = nil
	c.connMu.Unlock()
	c.connected.Store(false)
	if !c.closing.Load() && c.opts.IsNeedReconnect {
		c.startReconnect()
	}
}

func dialTLS(addr string, cfg *tls.Config, host string) (net.Conn, error) {
	if cfg == nil {
		cfg = &tls.Config{}
	}
	if cfg.ServerName == "" && host != "" {
		cfg = cfg.Clone()
		cfg.ServerName = host
	}
	dialer := &tls.Dialer{Config: cfg}
	return dialer.DialContext(context.Background(), "tcp", addr)
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

func errOrDefault(err, fallback error) error {
	if err != nil {
		return err
	}
	return fallback
}
