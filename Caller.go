package discover

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"apigo.cc/go/cast"
	gohttp "apigo.cc/go/http"
	"apigo.cc/go/log"
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
	appClientPools[app] = c
	return c
}

type Caller struct {
	Request *http.Request
	NoBody  bool
	logger  *log.Logger
}

func NewCaller(request *http.Request, logger *log.Logger) *Caller {
	return &Caller{Request: request, logger: logger}
}

func (c *Caller) logError(error string, extra ...any) {
	if c.logger == nil {
		c.logger = log.DefaultLogger
	}
	c.logger.Error("Discover Caller: "+error, extra...)
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

func (c *Caller) Open(app, path string, headers ...string) *websocket.Conn {
	r, _ := c.doWithNode(false, "WS", app, "", path, nil, headers...)
	if v, ok := r.(*websocket.Conn); ok {
		return v
	}
	return nil
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

func (c *Caller) doWithNode(manualDo bool, method, app, withNode, path string, data any, headers ...string) (any, string) {
	callerHeaders := make(map[string]string)
	for i := 1; i < len(headers); i += 2 {
		callerHeaders[headers[i-1]] = headers[i]
	}

	if isServer {
		callerHeaders[HeaderFromApp] = Config.App
		callerHeaders[HeaderFromNode] = myAddr
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

	settedHeaders := make([]string, 0, len(callerHeaders)*2)
	for k, v := range callerHeaders {
		settedHeaders = append(settedHeaders, k, v)
	}

	for {
		node := appClient.NextWithNode(app, withNode, c.Request)
		if node == nil {
			break
		}

		node.UsedTimes++
		startTime := time.Now()
		scheme := "http"
		if callInfo != nil && callInfo.SSL {
			scheme = "https"
		}

		hc := getHttpClient(app, callInfo.Timeout, callInfo.HttpVersion == 2 && !callInfo.SSL)
		hc.NoBody = c.NoBody

		var res *gohttp.Result
		var wsConn *websocket.Conn

		url := fmt.Sprintf("%s://%s%s", scheme, node.Addr, path)

		if strings.ToUpper(method) == "WS" {
			dialer := websocket.DefaultDialer
			h := http.Header{}
			for i := 1; i < len(settedHeaders); i += 2 {
				h.Set(settedHeaders[i-1], settedHeaders[i])
			}
			if scheme == "https" {
				scheme = "wss"
			} else {
				scheme = "ws"
			}
			wsUrl := fmt.Sprintf("%s://%s%s", scheme, node.Addr, path)
			conn, resp, err := dialer.Dial(wsUrl, h)
			wsConn = conn
			res = &gohttp.Result{Error: err, Response: resp}
		} else {
			if c.Request != nil {
				if manualDo {
					res = hc.ManualDoByRequest(c.Request, method, url, data, settedHeaders...)
				} else {
					res = hc.DoByRequest(c.Request, method, url, data, settedHeaders...)
				}
			} else {
				if manualDo {
					res = hc.ManualDo(method, url, data, settedHeaders...)
				} else {
					res = hc.Do(method, url, data, settedHeaders...)
				}
			}
		}

		responseTime := time.Since(startTime)
		settedLoadBalancer.Response(&appClient, node, res.Error, res.Response, responseTime)

		if res.Error != nil || (res.Response != nil && res.Response.StatusCode >= 502 && res.Response.StatusCode <= 504) {
			node.FailedTimes++
			errStr := ""
			if res.Error != nil {
				errStr = res.Error.Error()
			} else {
				errStr = res.Response.Status
			}

			c.logError(errStr, "app", app, "node", node.Addr, "path", path, "tryTimes", appClient.tryTimes)

			if node.FailedTimes >= Config.CallRetryTimes {
				logError("node removed due to high failures", "app", app, "node", node.Addr)
				if clientRedisPool != nil {
					clientRedisPool.Do("HDEL", app, node.Addr)
					clientRedisPool.PUBLISH("CH_"+app, fmt.Sprintf("%s 0", node.Addr))
				}
			}
			continue
		}

		if strings.ToUpper(method) == "WS" {
			return wsConn, node.Addr
		}
		return res, node.Addr
	}

	return &gohttp.Result{Error: fmt.Errorf("all nodes failed for %s %s", app, path)}, ""
}
