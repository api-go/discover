package discover

import (
	"net/http"

	gohttp "apigo.cc/go/http"
	"apigo.cc/go/jsmod"
)

func init() {
	jsmod.Register("discover", map[string]any{
		// RPC 调用 (消费默认实例)
		"get": func(app, path string, headers ...string) *jsResult {
			return wrapResult(Get(app, path, headers...))
		},
		"post": func(app, path string, data any, headers ...string) *jsResult {
			return wrapResult(Post(app, path, data, headers...))
		},
		"put": func(app, path string, data any, headers ...string) *jsResult {
			return wrapResult(DefaultDiscoverer.Put(app, path, data, headers...))
		},
		"delete": func(app, path string, data any, headers ...string) *jsResult {
			return wrapResult(DefaultDiscoverer.Delete(app, path, data, headers...))
		},

		// 链路透传
		"from": func(r *http.Request) *jsCaller {
			return &jsCaller{c: DefaultDiscoverer.From(r)}
		},
	})
}

// jsResult 包装 *gohttp.Result 以隐藏 Save() 等危险方法
type jsResult struct {
	r *gohttp.Result
}

func wrapResult(r *gohttp.Result) *jsResult {
	return &jsResult{r: r}
}

func (jr *jsResult) String() string { return jr.r.String() }
func (jr *jsResult) Bytes() []byte  { return jr.r.Bytes() }
func (jr *jsResult) Map() map[string]any {
	return jr.r.Map()
}
func (jr *jsResult) Slice() []any {
	return jr.r.Slice()
}
func (jr *jsResult) Status() int {
	if jr.r.Response != nil {
		return jr.r.Response.StatusCode
	}
	return 0
}
func (jr *jsResult) Error() string {
	if jr.r.Error != nil {
		return jr.r.Error.Error()
	}
	return ""
}

// jsCaller 包装 *Caller 以确保返回包装后的结果
type jsCaller struct {
	c *Caller
}

func (jc *jsCaller) Get(app, path string, headers ...string) *jsResult {
	return wrapResult(jc.c.Get(app, path, headers...))
}
func (jc *jsCaller) Post(app, path string, data any, headers ...string) *jsResult {
	return wrapResult(jc.c.Post(app, path, data, headers...))
}
func (jc *jsCaller) Put(app, path string, data any, headers ...string) *jsResult {
	return wrapResult(jc.c.Put(app, path, data, headers...))
}
func (jc *jsCaller) Delete(app, path string, data any, headers ...string) *jsResult {
	return wrapResult(jc.c.Delete(app, path, data, headers...))
}
