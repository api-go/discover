package discover_test

import (
	"context"
	"errors"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/api-go/discover"
	gohttp "github.com/api-go/http"
	"github.com/gorilla/websocket"
)

func websocketApp(t *testing.T, name, config string, servers ...*httptest.Server) string {
	t.Helper()
	discover.Init()
	discover.Stop()
	original := discover.Config
	discover.Config.Registry = ""
	discover.Config.Calls = maps.Clone(original.Calls)
	t.Cleanup(func() { discover.Stop(); discover.Config = original })
	app := "discover-open-" + name
	discover.AddExternalApp(app, config)
	for _, server := range servers {
		addr := strings.TrimPrefix(strings.TrimPrefix(server.URL, "https://"), "http://")
		discover.SetNode(app, addr, 100)
		t.Cleanup(func() { discover.SetNode(app, addr, 0) })
	}
	return app
}

func TestOpenBusinessRejectionDoesNotRetryOrEvict(t *testing.T) {
	for _, status := range []int{200, 301, 400, 401, 403, 404, 426, 429, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			var firstCalls, secondCalls atomic.Int32
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				firstCalls.Add(1)
				w.Header().Add("Set-Cookie", "one=1")
				w.Header().Add("Set-Cookie", "two=2")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, "rejected")
			}))
			defer first.Close()
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { secondCalls.Add(1); w.WriteHeader(403) }))
			defer second.Close()
			app := websocketApp(t, t.Name(), "2", first, second)
			addr := strings.TrimPrefix(first.URL, "http://")
			discover.SetLoadBalancer(&preferNode{addr: addr})
			defer discover.SetLoadBalancer(&discover.DefaultLoadBalancer{})
			for range 12 {
				conn, response, node, err := discover.NewCaller(nil, nil).Open(app, "/ws")
				if err == nil || conn != nil || response == nil || response.StatusCode != status || node != addr {
					t.Fatalf("handshake: %v %s %v", response, node, err)
				}
				if len(response.Header.Values("Set-Cookie")) != 2 {
					t.Fatal(response.Header)
				}
				response.Body.Close()
			}
			_, _, failed := discover.GetAppNodes(app)[addr].State()
			if failed != 0 || firstCalls.Load() != 12 || secondCalls.Load() != 0 {
				t.Fatalf("failed=%d calls=%d/%d", failed, firstCalls.Load(), secondCalls.Load())
			}
		})
	}
}

func TestOpenTransientFailureRetriesAndReturnsLastResponse(t *testing.T) {
	first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
		_, _ = io.WriteString(w, "unavailable")
	}))
	defer first.Close()
	second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			conn.Close()
		}
	}))
	defer second.Close()
	app := websocketApp(t, t.Name(), "2", first, second)
	addr := strings.TrimPrefix(first.URL, "http://")
	discover.SetLoadBalancer(&preferNode{addr: addr})
	defer discover.SetLoadBalancer(&discover.DefaultLoadBalancer{})
	conn, response, node, err := discover.NewCaller(nil, nil).Open(app, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	conn.Close()
	if node != strings.TrimPrefix(second.URL, "http://") || response.StatusCode != 101 {
		t.Fatalf("node=%s response=%v", node, response)
	}
	_, _, failed := discover.GetAppNodes(app)[addr].State()
	if failed != 1 {
		t.Fatalf("failure count=%d", failed)
	}
	discover.SetNode(app, node, 0)
	_, response, node, err = discover.NewCaller(nil, nil).Open(app, "/ws")
	if err == nil || response == nil || response.StatusCode != 503 || node != addr {
		t.Fatalf("last response=%v node=%s err=%v", response, node, err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if string(body) != "unavailable" {
		t.Fatalf("last body=%s", body)
	}
}

func TestOpenPreservesHeadersRouteTokenAndHandshake(t *testing.T) {
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		if len(r.Header.Values("X-Labels")) != 2 || len(r.Cookies()) != 2 || r.Header.Get("Access-Token") != "service-token" || r.URL.RequestURI() != "/routed?q=1" {
			t.Errorf("request=%s %v", r.URL, r.Header)
		}
		upgrader := websocket.Upgrader{Subprotocols: []string{"chat"}}
		conn, err := upgrader.Upgrade(w, r, http.Header{"Set-Cookie": {"one=1", "two=2"}})
		if err != nil {
			t.Error(err)
			return
		}
		defer conn.Close()
		for {
			kind, data, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if conn.WriteMessage(kind, data) != nil {
				return
			}
		}
	}))
	defer server.Close()
	app := websocketApp(t, t.Name(), "2:100ms:service-token", server)
	discover.SetRoute(func(client *discover.AppClient, _ *http.Request) {
		if client.App == "open-alias" {
			client.App = app
			client.Path = "/routed?q=1"
		}
	})
	defer discover.SetRoute(nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn, response, _, err := discover.NewCaller(nil, nil).OpenContext(ctx, "open-alias", "/ws", http.Header{"X-Labels": {"one"}, "Cookie": {"a=1", "b=2"}, "Sec-WebSocket-Protocol": {"chat"}}, http.Header{"X-Labels": {"two"}})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if len(response.Header.Values("Set-Cookie")) != 2 || conn.Conn.Subprotocol() != "chat" {
		t.Fatal(response.Header)
	}
	// A handshake timeout must not terminate an established connection.
	select {
	case <-time.After(150 * time.Millisecond):
	case <-done:
		t.Fatal("socket ended at handshake deadline")
	}
	_ = conn.SetReadDeadline(time.Now().Add(time.Second))
	if err := conn.Send("hello"); err != nil {
		t.Fatal(err)
	}
	if data, err := conn.ReadString(); err != nil || data != "hello" {
		t.Fatalf("echo=%s %v", data, err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation did not close socket")
	}
}

func TestOpenCanceledHandshakeDoesNotCountFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { cancel(); <-r.Context().Done() }))
	defer server.Close()
	app := websocketApp(t, t.Name(), "2", server)
	started := time.Now()
	_, _, _, err := discover.NewCaller(nil, nil).OpenContext(ctx, app, "/ws")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	if time.Since(started) > time.Second {
		t.Fatal("cancellation waited for handshake timeout")
	}
	node := discover.GetAppNodes(app)[strings.TrimPrefix(server.URL, "http://")]
	if node == nil {
		t.Fatal("fixture node was removed")
	}
	_, _, failed := node.State()
	if failed != 0 {
		t.Fatalf("failure count=%d", failed)
	}
}

func TestOpenWSSUsesInjectedPrivateCA(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err == nil {
			conn.Close()
		}
	}))
	defer server.Close()
	app := websocketApp(t, t.Name(), "s", server)
	client := gohttp.NewClient(time.Second)
	client.GetRawClient().Transport = server.Client().Transport
	caller := discover.NewCaller(nil, nil)
	caller.WebSocketClient = client
	conn, response, _, err := caller.Open(app, "/ws")
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if response.ProtoMajor != 1 {
		t.Fatal(response.Proto)
	}
}

func TestOpenCanceledHandshakePreservesExistingFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/fail" {
			w.WriteHeader(503)
			return
		}
		cancel()
		<-r.Context().Done()
	}))
	defer server.Close()
	app := websocketApp(t, t.Name(), "2", server)
	_, response, _, err := discover.NewCaller(nil, nil).Open(app, "/fail")
	if err == nil {
		t.Fatal("expected transient failure")
	}
	response.Body.Close()
	_, _, _, err = discover.NewCaller(nil, nil).OpenContext(ctx, app, "/cancel")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error=%v", err)
	}
	_, _, failed := discover.GetAppNodes(app)[strings.TrimPrefix(server.URL, "http://")].State()
	if failed != 1 {
		t.Fatalf("existing failure count=%d want=1", failed)
	}
}

func TestOpenDoesNotShareAppCookieJar(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if len(r.Cookies()) != 1 || r.Cookies()[0].Name != "user" {
			t.Errorf("shared cookie leaked: %v", r.Cookies())
		}
		upgrader := websocket.Upgrader{}
		conn, err := upgrader.Upgrade(w, r, http.Header{"Set-Cookie": {"private=one; Path=/"}})
		if err == nil {
			conn.Close()
		}
	}))
	defer server.Close()
	app := websocketApp(t, t.Name(), "2", server)
	for _, user := range []string{"one", "two"} {
		conn, response, _, err := discover.NewCaller(nil, nil).Open(app, "/ws", http.Header{"Cookie": {"user=" + user}})
		if err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("Set-Cookie") == "" {
			t.Fatal("response cookie lost")
		}
		conn.Close()
	}
}
