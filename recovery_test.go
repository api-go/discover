package discover_test

import (
	"github.com/api-go/discover"
	"github.com/api-go/redis"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRegistrationRecovery(t *testing.T) {
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		t.Skip("redis-server unavailable")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	discover.Init()
	original := discover.Config
	discover.Config.App = "discover-recovery-test"
	discover.Config.Registry = fmt.Sprintf("127.0.0.1:%d", port)
	discover.Config.Calls = map[string]string{"discover-recovery-test": "1"}
	defer func() { discover.Stop(); discover.Config = original }()
	health := func() int {
		response := httptest.NewRecorder()
		discover.HealthHandler().ServeHTTP(response, httptest.NewRequest("GET", discover.HealthPath, nil))
		return response.Code
	}
	before := time.Now()
	if !discover.Start("127.0.0.1:23456") {
		t.Fatal("recovery worker did not start")
	}
	if time.Since(before) > 200*time.Millisecond {
		t.Fatal("Start blocked on unavailable Redis")
	}
	if discover.IsRegistered() || health() != 503 {
		t.Fatal("unregistered server reported ready")
	}
	var process *exec.Cmd
	stopRedis := func() {
		if process != nil {
			process.Process.Kill()
			process.Wait()
			process = nil
		}
	}
	defer stopRedis()
	startRedis := func() {
		process = exec.Command(binary, "--bind", "127.0.0.1", "--port", fmt.Sprint(port), "--save", "", "--appendonly", "no")
		if err := process.Start(); err != nil {
			t.Fatal(err)
		}
	}
	await := func(ready bool) {
		t.Helper()
		timeout := time.NewTimer(8 * time.Second)
		defer timeout.Stop()
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			if discover.IsRegistered() == ready {
				return
			}
			select {
			case <-timeout.C:
				t.Fatalf("registration did not become %v", ready)
			case <-ticker.C:
			}
		}
	}
	startRedis()
	await(true)
	awaitNode(t, "discover-recovery-test", "127.0.0.1:23456")
	if health() != 200 {
		t.Fatal("recovered server not ready")
	}
	stopRedis()
	await(false)
	if health() != 503 {
		t.Fatal("lost registry reported ready")
	}
	startRedis()
	await(true)
	discover.Stop()
	discover.Wait()
	if discover.IsRegistered() {
		t.Fatal("stopped server registered")
	}
	stopRedis()
	if !discover.Start("127.0.0.1:23456") {
		t.Fatal("worker did not start")
	}
	discover.Stop()
	discover.Wait()
	startRedis()
	timer := time.NewTimer(1500 * time.Millisecond)
	defer timer.Stop()
	<-timer.C
	if discover.IsRegistered() {
		t.Fatal("stopped retry loop recovered")
	}
	registry := redis.GetRedis(discover.Config.Registry, nil)
	if registry.HEXISTS(discover.Config.App, "127.0.0.1:23456") || registry.EXISTS(discover.Config.App+"_127.0.0.1:23456") {
		t.Fatal("stopped node re-registered in Redis")
	}
}

func awaitNode(t *testing.T, app, addr string) {
	t.Helper()
	timeout := time.NewTimer(8 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if discover.GetAppNodes(app)[addr] != nil {
			return
		}
		select {
		case <-timeout.C:
			t.Fatal("node snapshot did not recover")
		case <-tick.C:
		}
	}
}
func awaitRegistered(t *testing.T) {
	t.Helper()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if discover.IsRegistered() {
			return
		}
		select {
		case <-timeout.C:
			t.Fatal("registration timed out")
		case <-tick.C:
		}
	}
}

// A TCP listener that accepts connections but never answers Redis commands models
// a hung server rather than the easier connection-refused failure.
func TestStartWithUnresponsiveRedis(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var connections sync.WaitGroup
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			connection, err := listener.Accept()
			if err != nil {
				return
			}
			connections.Add(1)
			go func() {
				defer connections.Done()
				defer connection.Close()
				buffer := make([]byte, 1024)
				for {
					if _, err := connection.Read(buffer); err != nil {
						return
					}
				}
			}()
		}
	}()
	defer func() { listener.Close(); <-closed; connections.Wait() }()
	discover.Init()
	original := discover.Config
	discover.Config.App = "go-discover-unresponsive"
	discover.Config.Registry = "redis://" + listener.Addr().String() + "/15"
	discover.Config.Calls = map[string]string{"go-discover-unresponsive": "1"}
	defer func() { discover.Stop(); discover.Config = original }()
	before := time.Now()
	if !discover.Start("127.0.0.1:31001") {
		t.Fatal("background lifecycle failed to start")
	}
	if time.Since(before) > 200*time.Millisecond {
		t.Fatal("Start waited for an unresponsive Redis")
	}
	tick := time.NewTimer(50 * time.Millisecond)
	defer tick.Stop()
	<-tick.C
	done := make(chan struct{})
	go func() { discover.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("Stop blocked on hung Redis")
	}
}

func TestCallFailuresDoNotDeleteGlobalRegistration(t *testing.T) {
	if os.Getenv("DISCOVER_REDIS_INTEGRATION") != "1" {
		t.Skip("set DISCOVER_REDIS_INTEGRATION=1")
	}
	var status atomic.Int32
	status.Store(503)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(int(status.Load())) }))
	defer backend.Close()
	addr := strings.TrimPrefix(backend.URL, "http://")
	app := fmt.Sprintf("go-discover-local-failure-%d", time.Now().UnixNano())
	discover.Init()
	original := discover.Config
	discover.Config.App = ""
	discover.Config.Registry = "redis://127.0.0.1:6379/15"
	discover.Config.Calls = map[string]string{app: "1"}
	discover.Config.CallRetryTimes = 1
	rd := redis.GetRedis(discover.Config.Registry, nil)
	if result := rd.Do("EVAL", `redis.call('HSET',KEYS[1],ARGV[1],100);redis.call('SETEX',KEYS[2],30,'1');return 1`, 2, app, app+"_"+addr, addr); result.Error != nil {
		t.Fatal(result.Error)
	}
	defer func() { discover.Stop(); discover.Config = original; rd.DEL(app); rd.DEL(app + "_" + addr) }()
	discover.Start("")
	awaitNode(t, app, addr)
	caller := discover.NewCaller(nil, nil)
	legacy := caller.Get(app, "/")
	if legacy.Response != nil && legacy.Response.Body != nil {
		legacy.Response.Body.Close()
	}
	if !rd.HEXISTS(app, addr) {
		t.Fatal("legacy caller deleted global membership")
	}
	request, _ := http.NewRequest("GET", "/", nil)
	response, _, _ := caller.DoRequest(app, request)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if !rd.HEXISTS(app, addr) {
		t.Fatal("native caller deleted global membership")
	}
	_, handshake, _, _ := caller.Open(app, "/")
	if handshake != nil && handshake.Body != nil {
		handshake.Body.Close()
	}
	if !rd.HEXISTS(app, addr) {
		t.Fatal("websocket caller deleted global membership")
	}
	status.Store(200)
	recovered := caller.Get(app, "/")
	if recovered.Response != nil && recovered.Response.Body != nil {
		defer recovered.Response.Body.Close()
	}
	if recovered.Error != nil || recovered.Response == nil || recovered.Response.StatusCode != 200 {
		t.Fatal("cached failed node was not probed after backend recovery")
	}
}
