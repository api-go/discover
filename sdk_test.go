package discover_test

import (
	"github.com/api-go/discover"
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHealthContract(t *testing.T) {
	defer discover.SetHealthHandler(nil)
	handler := discover.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, test := range []struct {
		method, path string
		code         int
		body         string
	}{{"GET", discover.HealthPath, 200, "OK"}, {"POST", discover.HealthPath, 405, ""}, {"GET", "/other", 204, ""}} {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(test.method, test.path, nil))
		if response.Code != test.code || response.Body.String() != test.body {
			t.Fatalf("%+v: %d %q", test, response.Code, response.Body.String())
		}
	}
	discover.SetHealthHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503); io.WriteString(w, "draining") }))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest("GET", discover.HealthPath, nil))
	if response.Code != 503 || response.Body.String() != "draining" {
		t.Fatal(response)
	}
}

func TestNativeMetadataAndMultiValueHeaders(t *testing.T) {
	discover.Init()
	original := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = original }()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		for name, expected := range map[string]string{"X-Real-IP": "192.0.2.4", "X-User-Agent": "test-browser", "X-Host": "entry.example", "X-Scheme": "http"} {
			if r.Header.Get(name) != expected {
				t.Errorf("%s: %q", name, r.Header.Get(name))
			}
		}
		if r.Header.Get("X-Request-ID") == "" || len(r.Header.Values("X-App-Labels")) != 2 {
			t.Error(r.Header)
		}
		if r.Header.Get("Access-Token") != "downstream" || r.Header.Get("Cookie") != "" {
			t.Error(r.Header)
		}
		io.WriteString(w, "OK")
	}))
	defer server.Close()
	app := "discover-sdk-headers"
	discover.AddExternalApp(app, "1:downstream")
	addr := strings.TrimPrefix(server.URL, "http://")
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	incoming := httptest.NewRequest("GET", "http://entry.example/", nil)
	incoming.RemoteAddr = "192.0.2.4:1234"
	incoming.Header.Set("User-Agent", "test-browser")
	incoming.Header["x-app-labels"] = []string{"a", "b"}
	incoming.Header.Set("Cookie", "secret")
	outgoing, _ := http.NewRequest("POST", "/submit", strings.NewReader("body"))
	response, _, err := discover.NewCaller(incoming, nil).DoRequest(app, outgoing)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
}

func TestConsecutiveFailuresAndPOSTRetry(t *testing.T) {
	discover.Init()
	original := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = original }()
	var status atomic.Int64
	status.Store(503)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != "payload" {
			t.Errorf("body=%q", body)
		}
		w.WriteHeader(int(status.Load()))
	}))
	defer server.Close()
	app := "discover-sdk-failures"
	discover.AddExternalApp(app, "1")
	addr := strings.TrimPrefix(server.URL, "http://")
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	caller := discover.NewCaller(nil, nil)
	call := func() {
		request, _ := http.NewRequest("POST", "/submit", bytes.NewBufferString("payload"))
		response, _, _ := caller.DoRequest(app, request)
		if response != nil {
			response.Body.Close()
		}
	}
	call()
	_, _, failed := discover.GetAppNodes(app)[addr].State()
	if failed != 1 {
		t.Fatal(failed)
	}
	status.Store(200)
	call()
	_, _, failed = discover.GetAppNodes(app)[addr].State()
	if failed != 0 {
		t.Fatal("success must reset", failed)
	}
	status.Store(501)
	call()
	_, _, failed = discover.GetAppNodes(app)[addr].State()
	if failed != 0 {
		t.Fatal("501 is not a node failure", failed)
	}
	status.Store(503)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if string(body) != "payload" {
			t.Errorf("replayed body=%q", body)
		}
		w.WriteHeader(201)
	}))
	defer healthy.Close()
	goodAddr := strings.TrimPrefix(healthy.URL, "http://")
	discover.SetNode(app, goodAddr, 100)
	defer discover.SetNode(app, goodAddr, 0)
	discover.SetLoadBalancer(&preferNode{addr: addr})
	defer discover.SetLoadBalancer(&discover.DefaultLoadBalancer{})
	request, _ := http.NewRequest("POST", "/submit", bytes.NewBufferString("payload"))
	response, node, err := caller.DoRequest(app, request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 201 || node != goodAddr {
		t.Fatal(response.StatusCode, node)
	}
}

type preferNode struct{ addr string }

func (p *preferNode) Next(_ *discover.AppClient, nodes []*discover.NodeInfo, _ *http.Request) *discover.NodeInfo {
	for _, n := range nodes {
		if n.Addr == p.addr {
			return n
		}
	}
	return nodes[0]
}
func (p *preferNode) Response(_ *discover.AppClient, _ *discover.NodeInfo, _ error, _ *http.Response, _ time.Duration) {
}

func TestNativeCancellationDoesNotCountFailure(t *testing.T) {
	discover.Init()
	original := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = original }()
	entered := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done() }))
	defer server.Close()
	app := "discover-sdk-cancel"
	discover.AddExternalApp(app, "1")
	addr := strings.TrimPrefix(server.URL, "http://")
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	ctx, cancel := context.WithCancel(context.Background())
	request, _ := http.NewRequestWithContext(ctx, "GET", "/", nil)
	done := make(chan error, 1)
	go func() { _, _, err := discover.NewCaller(nil, nil).DoRequest(app, request); done <- err }()
	<-entered
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("missing cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("cancel did not stop")
	}
	_, _, failed := discover.GetAppNodes(app)[addr].State()
	if failed != 0 {
		t.Fatal(failed)
	}
}

func TestNativeConcurrentCalls(t *testing.T) {
	discover.Init()
	original := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = original }()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	defer server.Close()
	app := "discover-sdk-concurrent"
	discover.AddExternalApp(app, "1")
	addr := strings.TrimPrefix(server.URL, "http://")
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			request, _ := http.NewRequest("GET", "/", nil)
			response, _, err := discover.NewCaller(nil, nil).DoRequest(app, request)
			if err != nil {
				t.Error(err)
				return
			}
			response.Body.Close()
		}()
	}
	wg.Wait()
	_, used, failed := discover.GetAppNodes(app)[addr].State()
	if used != 40 || failed != 0 {
		t.Fatal(fmt.Sprint(used, failed))
	}
}

func TestLegacyCallerPreservesAppHeaders(t *testing.T) {
	discover.Init()
	original := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = original }()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if values := r.Header.Values("X-App-Labels"); len(values) != 2 || values[0] != "a" || values[1] != "b" {
			t.Error(values)
		}
		if r.Header.Get("X-User-Agent") != "browser" {
			t.Error(r.Header)
		}
		io.WriteString(w, "OK")
	}))
	defer server.Close()
	app := "discover-sdk-legacy-headers"
	discover.AddExternalApp(app, "1")
	addr := strings.TrimPrefix(server.URL, "http://")
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	incoming := httptest.NewRequest("GET", "http://entry/", nil)
	incoming.Header.Set("User-Agent", "browser")
	incoming.Header["X-App-Labels"] = []string{"a", "b"}
	result := discover.NewCaller(incoming, nil).Get(app, "/")
	if result.Error != nil || result.String() != "OK" {
		t.Fatal(result.Error, result.String())
	}
}

func TestNativeResponseRemainsReadable(t *testing.T) {
	discover.Init()
	original := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = original }()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-release
		io.WriteString(w, "after-return")
	}))
	defer server.Close()
	app := "discover-sdk-stream"
	discover.AddExternalApp(app, "1")
	addr := strings.TrimPrefix(server.URL, "http://")
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	request, _ := http.NewRequest("GET", "/", nil)
	response, _, err := discover.NewCaller(nil, nil).DoRequest(app, request)
	close(release)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil || string(body) != "after-return" {
		t.Fatal(err, string(body))
	}
}

func TestLegacyStreamingResponseRemainsReadable(t *testing.T) {
	discover.Init()
	original := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = original }()
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		<-release
		io.WriteString(w, "stream")
	}))
	defer server.Close()
	app := "discover-sdk-legacy-stream"
	discover.AddExternalApp(app, "1")
	addr := strings.TrimPrefix(server.URL, "http://")
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	response, _ := discover.NewCaller(nil, nil).ManualDoWithNode("GET", app, "", "/", nil)
	close(release)
	if response.Error != nil {
		t.Fatal(response.Error)
	}
	defer response.Response.Body.Close()
	body, err := io.ReadAll(response.Response.Body)
	if err != nil || string(body) != "stream" {
		t.Fatal(err, string(body))
	}
}

func TestNativeLocalErrorDoesNotCountNodeFailure(t *testing.T) {
	discover.Init()
	original := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = original }()
	app := "discover-sdk-local-error"
	discover.AddExternalApp(app, "1")
	addr := "127.0.0.1:19998"
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	request, _ := http.NewRequest("GET", "/", nil)
	request.Header.Set("X-App-Test", "invalid\nvalue")
	_, _, err := discover.NewCaller(nil, nil).DoRequest(app, request)
	if err == nil {
		t.Fatal("invalid header accepted")
	}
	_, used, failed := discover.GetAppNodes(app)[addr].State()
	if used != 0 || failed != 0 {
		t.Fatal(used, failed)
	}
}

func TestNativeTimeoutCountsNodeFailure(t *testing.T) {
	discover.Init()
	original := discover.Config
	discover.Config.Registry = ""
	defer func() { discover.Config = original }()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer server.Close()
	app := "discover-sdk-timeout"
	discover.AddExternalApp(app, "20ms:1")
	addr := strings.TrimPrefix(server.URL, "http://")
	discover.SetNode(app, addr, 100)
	defer discover.SetNode(app, addr, 0)
	request, _ := http.NewRequest("GET", "/", nil)
	_, _, err := discover.NewCaller(nil, nil).DoRequest(app, request)
	if err == nil {
		t.Fatal("missing timeout")
	}
	_, _, failed := discover.GetAppNodes(app)[addr].State()
	if failed != 1 {
		t.Fatal(failed)
	}
}
