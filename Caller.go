package discover

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/api-go/cast"
	gohttp "github.com/api-go/http"
	"github.com/api-go/id"
	"github.com/api-go/log"
)

var appClientPools = make(map[string]*gohttp.Client)
var appClientPoolsLock sync.RWMutex

func getHttpClient(app string, timeout time.Duration, h2c bool) *gohttp.Client {
	appClientPoolsLock.RLock()
	c := appClientPools[app]
	appClientPoolsLock.RUnlock()
	if c != nil {
		return c
	}

	appClientPoolsLock.Lock()
	defer appClientPoolsLock.Unlock()
	c = appClientPools[app]
	if c != nil {
		return c
	}

	if h2c {
		c = gohttp.NewClientH2C(timeout)
	} else {
		c = gohttp.NewClient(timeout)
	}
	raw := c.GetRawClient()
	base := raw.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	raw.Transport = relayTransport{base: base}
	appClientPools[app] = c
	return c
}

type Caller struct {
	SuppressCallLog bool           // The caller provides its own equivalent proxy summary.
	WebSocketClient *gohttp.Client // Optional client configuration for Open.
	Request         *http.Request
	NoBody          bool
	logger          *log.Logger
	requestID       string
}

func NewCaller(request *http.Request, logger *log.Logger) *Caller {
	caller := &Caller{Request: request, logger: logger, requestID: id.MakeID(32)}
	if logger != nil {
		caller.requestID = logger.GetTraceId()
	}
	if request != nil {
		if incoming := request.Header.Get(HeaderRequestId); incoming != "" {
			caller.requestID = incoming
		}
	}
	return caller
}

func (c *Caller) logError(error string, extra ...any) {
	logger := c.logger
	if logger == nil {
		logger = log.DefaultLogger
	}
	logger.Error("Discover Caller: "+error, extra...)
}

func (c *Caller) Get(app, path string, headers ...string) *gohttp.Result {
	return c.Do("GET", app, path, nil, headers...)
}

func (c *Caller) Post(app, path string, data any, headers ...string) *gohttp.Result {
	return c.Do("POST", app, path, data, headers...)
}

func (c *Caller) Put(app, path string, data any, headers ...string) *gohttp.Result {
	return c.Do("PUT", app, path, data, headers...)
}

func (c *Caller) Delete(app, path string, data any, headers ...string) *gohttp.Result {
	return c.Do("DELETE", app, path, data, headers...)
}

func (c *Caller) Head(app, path string, headers ...string) *gohttp.Result {
	return c.Do("HEAD", app, path, nil, headers...)
}

func (c *Caller) Do(method, app, path string, data any, headers ...string) *gohttp.Result {
	r, _ := c.DoWithNode(method, app, "", path, data, headers...)
	return r
}

func (c *Caller) DoWithNode(method, app, withNode, path string, data any, headers ...string) (*gohttp.Result, string) {
	r, nodeAddr := c.doWithNode(false, method, app, withNode, path, data, headers...)
	if v, ok := r.(*gohttp.Result); ok {
		return v, nodeAddr
	}
	return nil, nodeAddr
}

func (c *Caller) ManualDoWithNode(method, app, withNode, path string, data any, headers ...string) (*gohttp.Result, string) {
	r, nodeAddr := c.doWithNode(true, method, app, withNode, path, data, headers...)
	if v, ok := r.(*gohttp.Result); ok {
		return v, nodeAddr
	}
	return nil, nodeAddr
}

func (c *Caller) doWithNode(manualDo bool, method, app, withNode, path string, data any, headers ...string) (returned any, returnedNode string) {
	started := time.Now()
	attempts := 0
	defer func() {
		var response *http.Response
		var err error
		if result, ok := returned.(*gohttp.Result); ok && result != nil {
			response = result.Response
			err = result.Error
		}
		phase := "complete"
		if manualDo || c.NoBody {
			phase = "headers"
		}
		c.callLog(started, app, method, path, phase, returnedNode, response, err, attempts)
	}()
	callerHeaders := make(map[string]string)
	for key, values := range c.relayHeaders() {
		if len(values) > 0 {
			callerHeaders[key] = values[0]
		}
	}
	for i := 1; i < len(headers); i += 2 {
		callerHeaders[headers[i-1]] = headers[i]
	}

	appLock.RLock()
	server, fromApp, fromNode := isServer, Config.App, myAddr
	appLock.RUnlock()
	if server {
		callerHeaders[HeaderFromApp] = fromApp
		callerHeaders[HeaderFromNode] = fromNode
	}

	callData := make(map[string]any)
	if data != nil && !c.NoBody {
		rv := cast.RealValue(reflect.ValueOf(data))
		if rv.Kind() == reflect.Map || rv.Kind() == reflect.Struct {
			cast.Convert(&callData, data)
		}
	}

	appClient := AppClient{
		Logger:  c.logger,
		App:     app,
		Method:  method,
		Path:    path,
		Data:    &callData,
		Headers: &callerHeaders,
	}

	if settedRoute != nil {
		settedRoute(&appClient, c.Request)
		app = appClient.App
		method = appClient.Method
		path = appClient.Path
	}

	if !appClient.CheckApp(app) {
		return &gohttp.Result{Error: fmt.Errorf("app %s not found", app)}, ""
	}

	callInfo := getCallInfo(app)
	if callInfo != nil && callInfo.Token != "" {
		callerHeaders["Access-Token"] = callInfo.Token
	}

	contextHeaders := c.relayHeaders()
	for key, value := range callerHeaders {
		if contextHeaders.Get(key) != value {
			contextHeaders.Set(key, value)
		}
	}
	// Explicit header arguments replace all incoming values for that field.
	for i := 1; i < len(headers); i += 2 {
		contextHeaders.Set(headers[i-1], headers[i])
	}
	handedOff := false
	cancelContext := func() {}
	defer func() {
		if !handedOff {
			cancelContext()
		}
	}()
	ctx := context.Background()
	if c.Request != nil {
		ctx = c.Request.Context()
	}
	if callInfo.Timeout > 0 {
		limited, cancel := context.WithTimeout(ctx, callInfo.Timeout)
		cancelContext = cancel
		ctx = limited
	}
	if server {
		contextHeaders.Set(HeaderFromApp, fromApp)
		contextHeaders.Set(HeaderFromNode, fromNode)
	}
	if callInfo.Token != "" {
		contextHeaders.Set("Access-Token", callInfo.Token)
	}
	ctx = withRelayHeaders(ctx, contextHeaders)
	retainResponse := func(res *gohttp.Result) {
		if (manualDo || c.NoBody) && res.Response != nil && res.Response.Body != nil {
			res.Response.Body = &nativeResponseBody{ReadCloser: res.Response.Body, release: cancelContext}
			handedOff = true
		}
	}
	settedHeaders := make([]string, 0, len(callerHeaders)*2)
	for k, v := range callerHeaders {
		settedHeaders = append(settedHeaders, k, v)
	}

	for {
		if err := ctx.Err(); err != nil {
			return &gohttp.Result{Error: err}, ""
		}
		node := appClient.NextWithNode(app, withNode, c.Request)
		if node == nil {
			break
		}

		attempts++
		node.beginCall()
		startTime := time.Now()
		scheme := "http"
		if callInfo != nil && callInfo.SSL {
			scheme = "https"
		}

		hc := getHttpClient(app, callInfo.Timeout, callInfo.HttpVersion == 2 && !callInfo.SSL)

		var res *gohttp.Result

		url := fmt.Sprintf("%s://%s%s", scheme, node.Addr, path)

		{
			if c.Request != nil {
				if manualDo || c.NoBody {
					res = hc.ManualDoContext(ctx, method, url, data, settedHeaders...)
				} else {
					res = hc.DoContext(ctx, method, url, data, settedHeaders...)
				}
			} else {
				if manualDo || c.NoBody {
					res = hc.ManualDoContext(ctx, method, url, data, settedHeaders...)
				} else {
					res = hc.DoContext(ctx, method, url, data, settedHeaders...)
				}
			}
		}

		responseTime := time.Since(startTime)
		settedLoadBalancer.Response(&appClient, node, res.Error, res.Response, responseTime)

		if res.Error != nil || (res.Response != nil && res.Response.StatusCode >= 502 && res.Response.StatusCode <= 504) {
			if errors.Is(res.Error, context.Canceled) || (c.Request != nil && c.Request.Context().Err() != nil) {
				retainResponse(res)
				return res, node.Addr
			}
			failedTimes := node.finishCall(true)
			errStr := ""
			if res.Error != nil {
				errStr = res.Error.Error()
			} else {
				errStr = res.Response.Status
			}

			c.logError(errStr, "app", app, "node", node.Addr, "path", path, "tryTimes", appClient.tryTimes)

			if failedTimes >= Config.CallRetryTimes {
				c.logError("node avoided locally due to high failures", "app", app, "node", node.Addr)
			}
			if _, streaming := data.(io.Reader); streaming {
				retainResponse(res)
				return res, node.Addr
			}
			if res.Response != nil && res.Response.Body != nil {
				res.Response.Body.Close()
			}
			continue
		}

		node.finishCall(false)
		retainResponse(res)
		return res, node.Addr
	}

	return &gohttp.Result{Error: fmt.Errorf("all nodes failed for %s %s", app, path)}, ""
}
