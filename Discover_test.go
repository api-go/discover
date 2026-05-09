package discover_test

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"apigo.cc/go/discover"
	"apigo.cc/go/redis"
)

func TestDiscover(t *testing.T) {
	// 启动一个模拟服务
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Skip("failed to listen, skipping test")
		return
	}
	addr := l.Addr().String()
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

	// 启动 Discover
	d := discover.Start("redis://127.0.0.1:6379/15", "test-app", addr, nil)
	if d == nil {
		t.Skip("failed to start discover (check redis), skipping test")
		return
	}
	defer d.Stop()

	// 添加外部应用调用配置
	d.AddExternalApp("test-app", discover.CallConfig{Timeout: time.Second})
	
	// 等待节点同步
	success := false
	for i := 0; i < 20; i++ {
		nodes := d.GetAppNodes("test-app")
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
	caller := d.NewCaller(nil, nil)
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
	rd := redis.GetRedis("redis://127.0.0.1:6379/15", nil)
	if rd.Error == nil {
		// 模拟发现新节点
		rd.PUBLISH("CH_test-app", "127.0.0.1:18002 100")
		
		success = false
		for i := 0; i < 20; i++ {
			nodes := d.GetAppNodes("test-app")
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

func BenchmarkDiscover(b *testing.B) {
	d := discover.New(nil)
	d.SetNode("bench-app", "127.0.0.1:8080", 100)
	d.SetNode("bench-app", "127.0.0.1:8081", 100)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		nodes := d.GetAppNodes("bench-app")
		if len(nodes) == 0 {
			b.Fatal("no node")
		}
	}
}
