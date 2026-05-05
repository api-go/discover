package discover_test

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"apigo.cc/go/discover"
	"apigo.cc/go/redis"
)

func TestDiscover(t *testing.T) {
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

func BenchmarkDiscover(b *testing.B) {
	discover.Config.App = "bench-app"
	discover.SetNode("bench-app", "127.0.0.1:8080", 100)
	discover.SetNode("bench-app", "127.0.0.1:8081", 100)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// 模拟一个不需要实际网络请求的调用过程，只测试 Discover 内部逻辑（负载均衡、节点选择等）
		// 我们通过 Mock 或直接调用内部方法来实现
		appClient := discover.AppClient{
			App:    "bench-app",
			Method: "GET",
			Path:   "/",
		}
		node := appClient.Next("bench-app", nil)
		if node == nil {
			b.Fatal("no node")
		}
	}
}
