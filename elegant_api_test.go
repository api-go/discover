package discover_test

import (
	"net"
	"net/http"
	"testing"
	"time"

	"apigo.cc/go/discover"
	gohttp "apigo.cc/go/http"
)

type TestResult struct {
	Message string `json:"message"`
}

func TestElegantAPI(t *testing.T) {
	// 1. 模拟服务
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	addr := l.Addr().String()
	mux := http.NewServeMux()
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"message":"ok"}`))
	})
	mux.HandleFunc("/post", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"message":"posted"}`))
	})
	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(l) }()
	defer server.Close()

	// 2. 配置并启动 Discover
	d := discover.Start("redis://127.0.0.1:6379/15", "api-test", addr, nil)
	if d == nil {
		t.Skip("redis not available")
	}
	defer d.Stop()

	// 添加外部应用调用配置
	d.AddExternalApp("api-test", discover.CallConfig{})

	// 等待节点同步
	for i := 0; i < 20; i++ {
		if nodes := d.GetAppNodes("api-test"); len(nodes) > 0 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	// 3. 测试调用并解析 (Stateless)
	res, err := gohttp.To[TestResult](d.Get("api-test", "/get"))
	if err != nil {
		t.Errorf("Get failed: %v", err)
	}
	if res.Message != "ok" {
		t.Errorf("unexpected message: %s", res.Message)
	}

	res2, err := gohttp.To[TestResult](d.Post("api-test", "/post", map[string]string{"foo": "bar"}))
	if err != nil {
		t.Errorf("Post failed: %v", err)
	}
	if res2.Message != "posted" {
		t.Errorf("unexpected message: %s", res2.Message)
	}

	// 4. 测试透传调用 (Stateful)
	req, _ := http.NewRequest("GET", "http://example.com", nil)
	req.Header.Set("X-Request-ID", "req-123")
	
	rawRes3 := d.From(req).Get("api-test", "/get")
	res3, err := gohttp.To[TestResult](rawRes3)
	if err != nil {
		t.Errorf("From(r).Get failed: %v", err)
	}
	if res3.Message != "ok" {
		t.Errorf("unexpected message: %s", res3.Message)
	}

	// 5. 测试直接获取 Result
	rawRes := d.Do("GET", "api-test", "/get", nil)
	if rawRes.Error != nil {
		t.Errorf("Do failed: %v", rawRes.Error)
	}
	if rawRes.String() != `{"message":"ok"}` {
		t.Errorf("unexpected raw string: %s", rawRes.String())
	}
}