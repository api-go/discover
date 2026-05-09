package discover

import (
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/gorilla/websocket"
	"apigo.cc/go/cast"
	gohttp "apigo.cc/go/http"
	"apigo.cc/go/log"
)

func (d *Discoverer) getHttpClient(app string, timeout time.Duration, h2c bool) *gohttp.Client {
	d.appClientPoolsLock.RLock()
	c := d.appClientPools[app]
	d.appClientPoolsLock.RUnlock()
	if c != nil {
		return c
	}

	d.appClientPoolsLock.Lock()
	defer d.appClientPoolsLock.Unlock()
	c = d.appClientPools[app]
	if c != nil {
		return c
	}

	if h2c {
		c = gohttp.NewClientH2C(timeout)
	} else {
		c = gohttp.NewClient(timeout)
	}
	d.appClientPools[app] = c
	return c
}

// Caller 用于发起服务间调用
type Caller struct {
	discoverer *Discoverer
	Request    *http.Request // 原始请求，用于透传 Header
	NoBody     bool          // 是否不发送请求体
	logger     *log.Logger   // 用于日志记录的 Logger
}

// NewCaller 创建一个新的调用器
func NewCaller(request *http.Request, logger *log.Logger) *Caller {
	return DefaultDiscoverer.NewCaller(request, logger)
}

// NewCaller 创建一个新的调用器实例
func (d *Discoverer) NewCaller(request *http.Request, logger *log.Logger) *Caller {
	return &Caller{discoverer: d, Request: request, logger: logger}
}

// logError 记录 Discover 调用器错误
func (c *Caller) logError(msg string, extra ...any) {
	if c.logger == nil {
		c.logger = log.DefaultLogger
	}
	c.logger.Error("Discover Caller: "+msg, extra...)
}

// Get 发起 GET 请求
func (c *Caller) Get(app, path string, headers ...string) *gohttp.Result {
	return c.Do("GET", app, path, nil, headers...)
}

// Post 发起 POST 请求
func (c *Caller) Post(app, path string, data any, headers ...string) *gohttp.Result {
	return c.Do("POST", app, path, data, headers...)
}

// Put 发起 PUT 请求
func (c *Caller) Put(app, path string, data any, headers ...string) *gohttp.Result {
	return c.Do("PUT", app, path, data, headers...)
}

// Delete 发起 DELETE 请求
func (c *Caller) Delete(app, path string, data any, headers ...string) *gohttp.Result {
	return c.Do("DELETE", app, path, data, headers...)
}

// Head 发起 HEAD 请求
func (c *Caller) Head(app, path string, headers ...string) *gohttp.Result {
	return c.Do("HEAD", app, path, nil, headers...)
}

// Call 发起通用的泛型请求并自动解析响应
func Call[T any](method, app, path string, data any, headers ...string) (T, error) {
	return CallT[T](DefaultDiscoverer.NewCaller(nil, nil), method, app, path, data, headers...)
}

// CallT 发起泛型请求并自动解析响应 (由于 Go 方法不支持泛型，故使用函数)
func CallT[T any](c *Caller, method, app, path string, data any, headers ...string) (T, error) {
	var result T
	res := c.Do(method, app, path, data, headers...)
	if res.Error != nil {
		return result, res.Error
	}
	err := res.To(&result)
	return result, err
}

// Do 发起通用请求
func (c *Caller) Do(method, app, path string, data any, headers ...string) *gohttp.Result {
	r, _ := c.DoWithNode(method, app, "", path, data, headers...)
	return r
}

// Open 发起 WebSocket 连接
func (c *Caller) Open(app, path string, headers ...string) *websocket.Conn {
	r, _ := c.doWithNode(false, "WS", app, "", path, nil, headers...)
	if v, ok := r.(*websocket.Conn); ok {
		return v
	}
	return nil
}

// DoWithNode 发起请求并返回结果及节点地址
func (c *Caller) DoWithNode(method, app, withNode, path string, data any, headers ...string) (*gohttp.Result, string) {
	r, nodeAddr := c.doWithNode(false, method, app, withNode, path, data, headers...)
	if v, ok := r.(*gohttp.Result); ok {
		return v, nodeAddr
	}
	return nil, nodeAddr
}

// ManualDoWithNode 发起请求（手动处理响应）并返回结果及节点地址
func (c *Caller) ManualDoWithNode(method, app, withNode, path string, data any, headers ...string) (*gohttp.Result, string) {
	r, nodeAddr := c.doWithNode(true, method, app, withNode, path, data, headers...)
	if v, ok := r.(*gohttp.Result); ok {
		return v, nodeAddr
	}
	return nil, nodeAddr
}

func (c *Caller) doWithNode(manual bool, method, app, withNode, path string, data any, headers ...string) (any, string) {
	callerHeaders := make(map[string]string)
	for i := 1; i < len(headers); i += 2 {
		callerHeaders[headers[i-1]] = headers[i]
	}

	conf := c.discoverer.GetConfig()
	if c.discoverer.isServer {
		callerHeaders[HeaderFromApp] = conf.App
		callerHeaders[HeaderFromNode] = c.discoverer.myAddr
	}

	callData := make(map[string]any)
	if data != nil && !c.NoBody {
		rv := cast.RealValue(reflect.ValueOf(data))
		if rv.Kind() == reflect.Map || rv.Kind() == reflect.Struct {
			cast.Convert(&callData, data)
		}
	}

	appClient := AppClient{
		discoverer: c.discoverer,
		Logger:     c.logger,
		App:        app,
		Method:     method,
		Path:       path,
		Data:       callData,
		Headers:    callerHeaders,
	}

	if c.discoverer.settedRoute != nil {
		c.discoverer.settedRoute(&appClient, c.Request)
		app = appClient.App
		method = appClient.Method
		path = appClient.Path
	}

	if !appClient.CheckApp(app) {
		return &gohttp.Result{Error: fmt.Errorf("app %s not found", app)}, ""
	}

	callInfo := c.discoverer.getCallInfo(app)
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

		node.UsedTimes.Add(1)
		startTime := time.Now()
		scheme := "http"
		if callInfo != nil && callInfo.SSL {
			scheme = "https"
		}

		hc := c.discoverer.getHttpClient(app, callInfo.Timeout, callInfo.HttpVersion == 2 && !callInfo.SSL)
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
				if manual {
					res = hc.ManualDoByRequest(c.Request, method, url, data, settedHeaders...)
				} else {
					res = hc.DoByRequest(c.Request, method, url, data, settedHeaders...)
				}
			} else {
				if manual {
					res = hc.ManualDo(method, url, data, settedHeaders...)
				} else {
					res = hc.Do(method, url, data, settedHeaders...)
				}
			}
		}

		responseTime := time.Since(startTime)
		usedTimeMs := float32(responseTime.Nanoseconds()) / 1e6
		c.discoverer.settedLoadBalancer.Response(&appClient, node, res.Error, res.Response, responseTime)

		if res.Error != nil || (res.Response != nil && res.Response.StatusCode >= 502 && res.Response.StatusCode <= 504) {
			node.FailedTimes.Add(1)
			errStr := ""
			if res.Error != nil {
				errStr = res.Error.Error()
			} else {
				errStr = res.Response.Status
			}

			c.logError(errStr, "app", app, "node", node.Addr, "path", path, "attempts", appClient.attempts)
			appClient.Log(node.Addr, usedTimeMs, fmt.Errorf("%s", errStr))

			if node.FailedTimes.Load() >= int32(conf.CallRetryTimes) {
				c.discoverer.logError("node isolated locally due to high failures", "app", app, "node", node.Addr)
			}
			continue
		}

		node.FailedTimes.Store(0)
		appClient.Log(node.Addr, usedTimeMs, nil)
		if strings.ToUpper(method) == "WS" {
			return wsConn, node.Addr
		}
		return res, node.Addr
	}

	return &gohttp.Result{Error: fmt.Errorf("all nodes failed for %s %s", app, path)}, ""
}
