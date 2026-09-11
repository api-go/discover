package discover_test

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"apigo.cc/go/discover"
	"apigo.cc/go/redis"
	"github.com/gorilla/websocket"
)

func TestDiscover(t *testing.T) {
	if os.Getenv("DISCOVER_REDIS_INTEGRATION") != "1" {
		t.Skip("set DISCOVER_REDIS_INTEGRATION=1 to run the Redis integration test")
	}
	// 启动一个模拟服务
	l, err := net.Listen("tcp", "127.0.0.1:18001")
	if err != nil {
		t.Skip("failed to listen on :18001, skipping test")
		return
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	upgrader := websocket.Upgrader{}
	mux.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			mt, message, err := conn.ReadMessage()
			if err != nil {
				break
			}
			_ = conn.WriteMessage(mt, message)
		}
	})

	server := &http.Server{Handler: mux}
	go func() { _ = server.Serve(l) }()
	defer server.Close()

	// 配置 Discover
	discover.Config.App = "test-app"
	discover.Config.Registry = "redis://127.0.0.1:6379/15"

	// 启动 Discover
	if !discover.Start("127.0.0.1:18001") {
		t.Skip("failed to start discover (check redis), skipping test")
		return
	}
	defer discover.Stop()

	// 添加外部应用调用配置
	discover.AddExternalApp("test-app", "1")

	// 等待节点同步
	success := false
	for i := 0; i < 20; i++ {
		nodes := discover.GetAppNodes("test-app")
		if len(nodes) > 0 {
			success = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !success {
		t.Fatal("node discovery timed out")
	}

	// 1. 使用 Caller 调用 HTTP
	caller := discover.NewCaller(nil, nil)
	res := caller.Get("test-app", "/")
	if res.Error != nil {
		t.Errorf("http call failed: %v", res.Error)
	}
	if res.String() != "OK" {
		t.Errorf("unexpected http response: %s", res.String())
	}

	// 2. 使用 Caller 调用 WebSocket
	wsConn := caller.Open("test-app", "/ws")
	if wsConn == nil {
		t.Fatal("websocket open failed")
	}
	defer wsConn.Close()

	msg := []byte("hello")
	if err := wsConn.WriteMessage(websocket.TextMessage, msg); err != nil {
		t.Fatalf("ws write failed: %v", err)
	}

	_, reply, err := wsConn.ReadMessage()
	if err != nil {
		t.Fatalf("ws read failed: %v", err)
	}
	if string(reply) != "hello" {
		t.Errorf("unexpected ws reply: %s", string(reply))
	}

	// 3. 测试负载均衡和节点更新
	rd := redis.GetRedis(discover.Config.Registry, nil)
	if rd.Error == nil {
		// 模拟发现新节点
		rd.PUBLISH("CH_test-app", "127.0.0.1:18002 100")

		success = false
		for i := 0; i < 20; i++ {
			nodes := discover.GetAppNodes("test-app")
			if len(nodes) >= 2 {
				success = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		if !success {
			t.Error("node update sync failed")
		}
	}
}

func TestEasyStart(t *testing.T) {
	if os.Getenv("DISCOVER_REDIS_INTEGRATION") != "1" {
		t.Skip("set DISCOVER_REDIS_INTEGRATION=1 to run the Redis integration test")
	}
	// 模拟环境变量
	_ = os.Setenv("DISCOVER_APP", "test-app")
	_ = os.Setenv("DISCOVER_LISTEN", "18003")
	_ = os.Setenv("DISCOVER_REGISTRY", "redis://127.0.0.1:6379/15")

	ip, port := discover.EasyStart()
	if ip == "" || port == 0 {
		t.Skip("EasyStart failed (check redis), skipping test")
		return
	}
	fmt.Printf("EasyStart: %s:%d\n", ip, port)
	discover.Stop()
}

func BenchmarkDefaultLoadBalancer(b *testing.B) {
	loadBalancer := &discover.DefaultLoadBalancer{}
	appClient := &discover.AppClient{}
	nodes := []*discover.NodeInfo{
		{Addr: "127.0.0.1:18001", Weight: 100},
		{Addr: "127.0.0.1:18002", Weight: 200},
		{Addr: "127.0.0.1:18003", Weight: 300},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		loadBalancer.Next(appClient, nodes, nil)
	}
}
