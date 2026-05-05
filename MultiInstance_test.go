package discover_test

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"apigo.cc/go/discover"
)

func TestMultipleDiscoverer(t *testing.T) {
	// 启动两个模拟服务
	l1, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l1: %v", err)
	}
	addr1 := l1.Addr().String()
	mux1 := http.NewServeMux()
	mux1.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("OK1")) })
	server1 := &http.Server{Handler: mux1}
	go func() { _ = server1.Serve(l1) }()
	defer server1.Close()

	l2, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen l2: %v", err)
	}
	addr2 := l2.Addr().String()
	mux2 := http.NewServeMux()
	mux2.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("OK2")) })
	server2 := &http.Server{Handler: mux2}
	go func() { _ = server2.Serve(l2) }()
	defer server2.Close()

	registry := "redis://127.0.0.1:6379/15"

	// 实例 1
	d1 := discover.NewDiscoverer()
	c1conf := d1.GetConfig()
	c1conf.App = "app1"
	c1conf.Registry = registry + "?id=1"
	d1.SetConfig(c1conf)
	if !d1.Start(addr1) {
		t.Skip("redis not available")
	}
	defer d1.Stop()

	// 实例 2
	d2 := discover.NewDiscoverer()
	c2conf := d2.GetConfig()
	c2conf.App = "app2"
	c2conf.Registry = registry + "?id=2"
	d2.SetConfig(c2conf)
	if !d2.Start(addr2) {
		t.Skip("redis not available")
	}
	defer d2.Stop()

	// 实例 1 发现并调用自己
	d1.AddExternalApp("app1", "1")
	time.Sleep(200 * time.Millisecond) // 等待同步
	c1 := d1.NewCaller(nil, nil)
	res1 := c1.Get("app1", "/")
	if res1.Error != nil || res1.String() != "OK1" {
		t.Errorf("d1 call app1 failed: %v, %s", res1.Error, res1.String())
	}

	// 实例 2 发现并调用 实例 1
	d2.AddExternalApp("app1", "1")
	time.Sleep(200 * time.Millisecond) // 等待同步
	c2 := d2.NewCaller(nil, nil)
	res2 := c2.Get("app1", "/")
	if res2.Error != nil || res2.String() != "OK1" {
		t.Errorf("d2 call app1 failed: %v, %s", res2.Error, res2.String())
	}

	// 验证：d1 也可以调用 app2，只要正确配置
	d1.AddExternalApp("app2", "1")
	time.Sleep(200 * time.Millisecond) // 等待同步
	res3 := c1.Get("app2", "/")
	if res3.Error != nil || res3.String() != "OK2" {
		t.Errorf("d1 call app2 failed: %v, %s", res3.Error, res3.String())
	}

	fmt.Println("Multiple Discoverer instances verified")
}
