package discover

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	gohttp "github.com/api-go/http"
	"golang.org/x/net/http/httpguts"
)

// Open establishes a dedicated HTTP/1.1 WebSocket connection. The response and
// selected node are returned on both success and rejection. Close response.Body
// after inspecting a rejection. The parent context governs the socket lifetime;
// the discovery timeout only bounds handshake attempts.
func (c *Caller) Open(app, path string, headers ...http.Header) (*gohttp.WebSocketConn, *http.Response, string, error) {
	ctx := context.Background()
	if c.Request != nil {
		ctx = c.Request.Context()
	}
	return c.OpenContext(ctx, app, path, headers...)
}

func (c *Caller) OpenContext(ctx context.Context, app, path string, headers ...http.Header) (*gohttp.WebSocketConn, *http.Response, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	fields := c.relayHeaders()
	replaced := make(map[string]bool)
	for _, explicit := range headers {
		for key, values := range explicit {
			canonical := http.CanonicalHeaderKey(key)
			if !replaced[canonical] {
				fields.Del(canonical)
				replaced[canonical] = true
			}
			fields[canonical] = append(fields[canonical], values...)
		}
	}
	flat := make(map[string]string, len(fields))
	for key, values := range fields {
		if len(values) > 0 {
			flat[key] = values[0]
		}
	}
	data := make(map[string]any)
	appClient := &AppClient{Logger: c.logger, App: app, Method: "WS", Path: path, Data: &data, Headers: &flat}
	if settedRoute != nil {
		settedRoute(appClient, c.Request)
		app, path = appClient.App, appClient.Path
		if !strings.EqualFold(appClient.Method, "WS") {
			return nil, nil, "", fmt.Errorf("WebSocket route must retain method WS")
		}
		if appClient.Headers != nil {
			flat = *appClient.Headers
		}
	}
	for key := range fields {
		if _, exists := flat[key]; !exists {
			fields.Del(key)
		}
	}
	for key, value := range flat {
		if fields.Get(key) != value {
			fields.Set(key, value)
		}
	}
	if !appClient.CheckApp(app) {
		return nil, nil, "", fmt.Errorf("app %s not found", app)
	}
	info := getCallInfo(app)
	if info == nil {
		return nil, nil, "", fmt.Errorf("app %s has no call configuration", app)
	}
	appLock.RLock()
	server, fromApp, fromNode := isServer, Config.App, myAddr
	appLock.RUnlock()
	if server {
		fields.Set(HeaderFromApp, fromApp)
		fields.Set(HeaderFromNode, fromNode)
	}
	if info.Token != "" {
		fields.Set("Access-Token", info.Token)
	}
	// The dialer owns all hop-by-hop and handshake-generated fields.
	for _, token := range strings.Split(fields.Get("Connection"), ",") {
		fields.Del(strings.TrimSpace(token))
	}
	for _, key := range []string{"Connection", "Upgrade", "Proxy-Connection", "Keep-Alive", "Proxy-Authorization", "Proxy-Authenticate", "Te", "Trailer", "Transfer-Encoding", "Sec-WebSocket-Key", "Sec-WebSocket-Version", "Sec-WebSocket-Extensions"} {
		fields.Del(key)
	}
	for name, values := range fields {
		if !httpguts.ValidHeaderFieldName(name) {
			return nil, nil, "", fmt.Errorf("invalid header name")
		}
		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return nil, nil, "", fmt.Errorf("invalid header value")
			}
		}
	}
	parsed, err := url.Parse(path)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || !strings.HasPrefix(path, "/") || parsed.Fragment != "" {
		return nil, nil, "", fmt.Errorf("invalid WebSocket path %q", path)
	}
	base := c.WebSocketClient
	if base == nil {
		base = getHttpClient(app, info.Timeout, info.HttpVersion == 2 && !info.SSL).Clone()
		// The app pool is shared by unrelated callers. WebSocket previously had
		// no jar; retain that boundary unless a caller explicitly injects one.
		base.GetRawClient().Jar = nil
		base.GetRawClient().Timeout = info.Timeout
		dialer := base.WebSocketDialer()
		dialer.Jar = nil
		base.SetWebSocketDialer(dialer)
	}
	deadline := time.Time{}
	if info.Timeout > 0 {
		deadline = time.Now().Add(info.Timeout)
	}
	var lastResponse *http.Response
	var lastError error
	var lastNode string
	for {
		if err := ctx.Err(); err != nil {
			if lastResponse != nil && lastResponse.Body != nil {
				lastResponse.Body.Close()
			}
			return nil, nil, lastNode, err
		}
		if !deadline.IsZero() && time.Now().After(deadline) {
			if lastError != nil {
				return nil, lastResponse, lastNode, lastError
			}
			return nil, nil, lastNode, context.DeadlineExceeded
		}
		node := appClient.Next(app, c.Request)
		if node == nil {
			if lastError != nil {
				return nil, lastResponse, lastNode, lastError
			}
			return nil, nil, "", fmt.Errorf("all nodes failed for %s %s", app, path)
		}
		if lastResponse != nil && lastResponse.Body != nil {
			lastResponse.Body.Close()
			lastResponse = nil
		}
		client := base.Clone()
		if !deadline.IsZero() {
			remaining := time.Until(deadline)
			if client.GetRawClient().Timeout <= 0 || remaining < client.GetRawClient().Timeout {
				client.GetRawClient().Timeout = remaining
			}
		}
		target := *parsed
		target.Scheme = "ws"
		if info.SSL {
			target.Scheme = "wss"
		}
		target.Host = node.Addr
		node.beginCall()
		started := time.Now()
		conn, response, openErr := client.OpenContext(ctx, target.String(), fields)
		// HTTP rejections indicate a live server. Only connectivity failures and
		// transient gateway errors participate in failover and node eviction.
		retry := openErr != nil && response == nil || response != nil && response.StatusCode >= 502 && response.StatusCode <= 504
		canceled := ctx.Err() != nil || errors.Is(openErr, context.Canceled)
		if !canceled {
			node.finishCall(retry)
		}
		settedLoadBalancer.Response(appClient, node, openErr, response, time.Since(started))
		if canceled {
			if conn != nil {
				conn.Close()
			}
			if response != nil && response.Body != nil {
				response.Body.Close()
			}
			cancelErr := ctx.Err()
			if cancelErr == nil {
				cancelErr = openErr
			}
			return nil, nil, node.Addr, cancelErr
		}
		if !retry {
			return conn, response, node.Addr, openErr
		}
		lastResponse, lastError, lastNode = response, openErr, node.Addr
		if lastError == nil {
			lastError = fmt.Errorf("WebSocket upstream returned %s", response.Status)
		}
		c.logError(lastError.Error(), "app", app, "node", node.Addr, "path", path, "tryTimes", appClient.tryTimes)
		// Failure scores are local; only the owner or an expired lease removes membership.

	}
}
