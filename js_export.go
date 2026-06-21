package discover

import (
	"net/http"

	gohttp "apigo.cc/go/http"
	"apigo.cc/go/jsmod"
)

func init() {
	jsmod.Register("discover", map[string]any{
		// RPC 调用 (全大写对齐 HTTP)
		"GET":    jsGet,
		"POST":   jsPost,
		"PUT":    jsPut,
		"DELETE": jsDelete,

		// 链路透传
		"From": jsFrom,
	})
}

func jsGet(app, path string, headers map[string]string) *jsResult {
	return wrapResult(Get(app, path, flattenHeaders(headers)...))
}

func jsPost(app, path string, data any, headers map[string]string) *jsResult {
	return wrapResult(Post(app, path, data, flattenHeaders(headers)...))
}

func jsPut(app, path string, data any, headers map[string]string) *jsResult {
	return wrapResult(DefaultDiscoverer.Put(app, path, data, flattenHeaders(headers)...))
}

func jsDelete(app, path string, data any, headers map[string]string) *jsResult {
	return wrapResult(DefaultDiscoverer.Delete(app, path, data, flattenHeaders(headers)...))
}

func jsFrom(r *http.Request) *jsCaller {
	return &jsCaller{c: DefaultDiscoverer.From(r)}
}

func flattenHeaders(m map[string]string) []string {
	if len(m) == 0 {
		return nil
	}
	res := make([]string, 0, len(m)*2)
	for k, v := range m {
		res = append(res, k, v)
	}
	return res
}

// jsResult 包装 *gohttp.Result 以对齐 PascalCase
type jsResult struct {
	r *gohttp.Result
}

func wrapResult(r *gohttp.Result) *jsResult {
	if r != nil && r.Error != nil {
		r.Error = jsmod.MakeError(r.Error)
	}
	return &jsResult{r: r}
}

func (jr *jsResult) String() string      { return jr.r.String() }
func (jr *jsResult) Bytes() []byte       { return jr.r.Bytes() }
func (jr *jsResult) Map() map[string]any { return jr.r.Map() }
func (jr *jsResult) Slice() []any        { return jr.r.Slice() }
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

// jsCaller 包装 *Caller 以对齐全大写方法
type jsCaller struct {
	c *Caller
}

func (jc *jsCaller) GET(app, path string, headers map[string]string) *jsResult {
	return wrapResult(jc.c.Get(app, path, flattenHeaders(headers)...))
}
func (jc *jsCaller) POST(app, path string, data any, headers map[string]string) *jsResult {
	return wrapResult(jc.c.Post(app, path, data, flattenHeaders(headers)...))
}
func (jc *jsCaller) PUT(app, path string, data any, headers map[string]string) *jsResult {
	return wrapResult(jc.c.Put(app, path, data, flattenHeaders(headers)...))
}
func (jc *jsCaller) DELETE(app, path string, data any, headers map[string]string) *jsResult {
	return wrapResult(jc.c.Delete(app, path, data, flattenHeaders(headers)...))
}
