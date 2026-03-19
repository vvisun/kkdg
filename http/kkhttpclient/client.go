package kkhttpclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

type Response struct {
	StatusCode int
	Header     http.Header
	Body       []byte
}

type Result struct {
	Resp Response
	Err  error
}

type Client struct {
	opt        ClientOptions
	httpClient *http.Client
}

func NewClient(opts ...ClientOption) *Client {
	o := defaultClientOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Client{
		opt: o,
		httpClient: &http.Client{
			Timeout: o.Timeout,
		},
	}
}

func (c *Client) buildURL(pathOrURL string) (string, error) {
	if pathOrURL == "" {
		return "", errors.New("pathOrURL is empty")
	}
	// If already absolute URL, use it.
	if u, err := url.Parse(pathOrURL); err == nil && u.IsAbs() {
		return pathOrURL, nil
	}
	if c.opt.BaseURL == "" {
		return "", fmt.Errorf("BaseURL is empty and pathOrURL is not absolute: %s", pathOrURL)
	}

	base := strings.TrimRight(c.opt.BaseURL, "/")
	path := strings.TrimLeft(pathOrURL, "/")
	return base + "/" + path, nil
}

// SyncRequest executes the HTTP request synchronously.
func (c *Client) SyncRequest(ctx context.Context, method, pathOrURL string, headers map[string]string, body []byte) (Response, error) {
	fullURL, err := c.buildURL(pathOrURL)
	if err != nil {
		return Response{}, err
	}

	var r io.Reader
	if body != nil {
		// Copy to avoid callers mutating body while the request is in-flight.
		buf := make([]byte, len(body))
		copy(buf, body)
		r = bytes.NewReader(buf)
	}

	req, err := http.NewRequestWithContext(ctx, method, fullURL, r)
	if err != nil {
		return Response{}, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return Response{}, err
	}

	result := Response{
		StatusCode: resp.StatusCode,
		Header:     resp.Header.Clone(),
		Body:       respBody,
	}

	if c.opt.StrictStatus && (resp.StatusCode < 200 || resp.StatusCode >= 300) {
		// Keep error small: first 1KB body.
		msg := string(respBody)
		if len(msg) > 1024 {
			msg = msg[:1024]
		}
		return result, fmt.Errorf("http status %d: %s", resp.StatusCode, msg)
	}

	return result, nil
}

// AsyncRequest executes the HTTP request asynchronously.
// It returns a channel with exactly one Result.
func (c *Client) AsyncRequest(ctx context.Context, method, pathOrURL string, headers map[string]string, body []byte) <-chan Result {
	ch := make(chan Result, 1)
	go func() {
		defer func() { close(ch) }()
		resp, err := c.SyncRequest(ctx, method, pathOrURL, headers, body)
		ch <- Result{Resp: resp, Err: err}
	}()
	return ch
}

// SyncJSON is a convenience helper:
// - reqObj will be JSON-marshaled (if not nil)
// - respObj will be JSON-unmarshaled (if not nil)
func (c *Client) SyncJSON(ctx context.Context, method, pathOrURL string, headers map[string]string, reqObj any, respObj any) error {
	var body []byte
	var contentType string
	if reqObj != nil {
		b, err := json.Marshal(reqObj)
		if err != nil {
			return err
		}
		body = b
		contentType = "application/json"
	}

	if headers == nil {
		headers = make(map[string]string, 1)
	}
	if contentType != "" && headers["Content-Type"] == "" {
		headers["Content-Type"] = contentType
	}

	res, err := c.SyncRequest(ctx, method, pathOrURL, headers, body)
	if err != nil {
		return err
	}
	if respObj == nil {
		return nil
	}
	return json.Unmarshal(res.Body, respObj)
}
