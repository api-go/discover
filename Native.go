package discover

import (
	"context"
	"errors"
	"fmt"
	"golang.org/x/net/http/httpguts"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var nativeLock sync.RWMutex
var nativeClient *http.Client

// SetHTTPClient supplies a native transport before serving requests. nil restores defaults.
// Caller owns a supplied client's lifecycle; the SDK does not close it.
func SetHTTPClient(client *http.Client) {
	nativeLock.Lock()
	nativeClient = client
	nativeLock.Unlock()
}

// DoRequest calls app using a native outbound request. URL must have a local absolute path.
// Request body is replayed only when GetBody is provided; responses are owned by the caller.
// Incoming headers come from NewCaller.Request, then explicit outgoing headers override them.
func (c *Caller) DoRequest(app string, request *http.Request) (*http.Response, string, error) {
	if request == nil || request.URL == nil || request.URL.Path == "" || request.URL.Path[0] != '/' || request.URL.IsAbs() || request.URL.Host != "" {
		return nil, "", fmt.Errorf("outbound request requires a local absolute path")
	}
	headers := c.relayHeaders()
	for name, values := range request.Header {
		headers[http.CanonicalHeaderKey(name)] = append([]string(nil), values...)
	}
	routeHeaders := map[string]string{}
	for name, values := range headers {
		if len(values) > 0 {
			routeHeaders[name] = values[0]
		}
	}
	routeData := map[string]any{} // Native bodies are opaque; routing may modify metadata only.
	ac := &AppClient{App: app, Method: request.Method, Path: request.URL.RequestURI(), Logger: c.logger, Headers: &routeHeaders, Data: &routeData}
	if settedRoute != nil {
		settedRoute(ac, c.Request)
	}
	app = ac.App
	targetPath, parseError := url.ParseRequestURI(ac.Path)
	if parseError != nil || targetPath.IsAbs() || targetPath.Host != "" || !strings.HasPrefix(ac.Path, "/") || strings.HasPrefix(ac.Path, "//") {
		return nil, "", fmt.Errorf("route must produce a local absolute path")
	}
	for name := range headers {
		if _, ok := routeHeaders[name]; !ok {
			headers.Del(name)
		}
	}
	for name, value := range routeHeaders {
		if headers.Get(name) != value {
			headers.Set(name, value)
		}
	}

	if !ac.CheckApp(app) {
		return nil, "", fmt.Errorf("app %s not found", app)
	}
	info := getCallInfo(app)
	timeout := 10 * time.Second
	if info != nil {
		timeout = info.Timeout
	}
	cleanup := []func(){}
	handedOff := false
	release := func() {
		for _, fn := range cleanup {
			fn()
		}
	}
	defer func() {
		if !handedOff {
			release()
		}
	}()
	ctx := request.Context()
	if c.Request != nil { // Preserve both inbound cancellation and outbound cancellation.
		linked, cancel := context.WithCancel(ctx)
		stop := context.AfterFunc(c.Request.Context(), cancel)
		cleanup = append(cleanup, func() { stop(); cancel() })
		ctx = linked
	}
	if timeout > 0 {
		limited, cancel := context.WithTimeout(ctx, timeout)
		cleanup = append(cleanup, cancel)
		ctx = limited
	}

	appLock.RLock()
	server, fromApp, fromNode := isServer, Config.App, myAddr
	appLock.RUnlock()
	if server {
		headers.Set(HeaderFromApp, fromApp)
		headers.Set(HeaderFromNode, fromNode)
	}
	if info != nil && info.Token != "" {
		headers.Set("Access-Token", info.Token)
	}
	if !httpguts.ValidHeaderFieldName(ac.Method) {
		return nil, "", fmt.Errorf("invalid HTTP method")
	}
	for name, values := range headers {
		if !httpguts.ValidHeaderFieldName(name) {
			return nil, "", fmt.Errorf("invalid header name")
		}
		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return nil, "", fmt.Errorf("invalid header value")
			}
		}
	}
	first := true
	for {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		node := ac.Next(app, c.Request)
		if node == nil {
			return nil, "", fmt.Errorf("all nodes failed for %s %s", app, request.URL.Path)
		}
		outbound := request.Clone(ctx)
		outbound.Header = headers.Clone()
		outbound.Method = ac.Method
		outbound.RequestURI = ""
		outbound.Host = ""
		scheme := "http"
		if info != nil && info.SSL {
			scheme = "https"
		}
		target := *targetPath
		target.Scheme = scheme
		target.Host = node.Addr
		outbound.URL = &target
		if !first && request.Body != nil && request.Body != http.NoBody {
			if request.GetBody == nil {
				return nil, node.Addr, fmt.Errorf("request body cannot be replayed")
			}
			body, err := request.GetBody()
			if err != nil {
				return nil, node.Addr, err
			}
			outbound.Body = body
		}
		first = false
		nativeLock.RLock()
		client := nativeClient
		nativeLock.RUnlock()
		if client == nil {
			h2c := info != nil && info.HttpVersion == 2 && !info.SSL
			client = getHttpClient(app, timeout, h2c).GetRawClient()
		}
		node.beginCall()
		started := time.Now()
		response, err := client.Do(outbound)
		settedLoadBalancer.Response(ac, node, err, response, time.Since(started))
		parentCanceled := request.Context().Err() != nil || c.Request != nil && c.Request.Context().Err() != nil
		if parentCanceled || errors.Is(err, context.Canceled) {
			if response != nil && response.Body != nil {
				response.Body.Close()
			}
			cancelError := ctx.Err()
			if cancelError == nil {
				cancelError = err
			}
			return nil, node.Addr, cancelError
		}
		failed := err != nil || response != nil && response.StatusCode >= 502 && response.StatusCode <= 504
		node.finishCall(failed)
		if !failed {
			if response != nil && response.Body != nil {
				response.Body = &nativeResponseBody{ReadCloser: response.Body, release: release}
				handedOff = true
			}
			return response, node.Addr, err
		}
		// Failure scores are local; only the owner or an expired lease removes membership.

		// A streaming body without a factory is never replayed, even for retryable statuses.
		if request.Body != nil && request.Body != http.NoBody && request.GetBody == nil {
			if response != nil && response.Body != nil {
				response.Body = &nativeResponseBody{ReadCloser: response.Body, release: release}
				handedOff = true
			}
			return response, node.Addr, err
		}
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
	}
}

type nativeResponseBody struct {
	io.ReadCloser
	once    sync.Once
	release func()
}

func (body *nativeResponseBody) Close() error {
	err := body.ReadCloser.Close()
	body.once.Do(body.release)
	return err
}
