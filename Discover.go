package discover

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/api-go/cast"
	"github.com/api-go/config"
	"github.com/api-go/id"
	"github.com/api-go/log"
	"github.com/api-go/rand"
	"github.com/api-go/redis"
)

var (
	serverRedisPool *redis.Redis
	clientRedisPool *redis.Redis
	pubsubRedisPool *redis.Redis
	isServer        = false
	isClient        = false
	registered      = false
	activeApp       string
	reconcileWake   chan struct{}
	myAddr          = ""
	_logger         = log.DefaultLogger
	_inited         = false

	lifecycleLock sync.Mutex
	heartbeatStop chan struct{}
	heartbeatDone chan struct{}
	appLock       sync.RWMutex
	_calls        = map[string]*callInfoType{}
	_appNodes     = map[string]map[string]*NodeInfo{}
	appSubscribed = map[string]bool{}

	settedRoute        func(*AppClient, *http.Request) = nil
	settedLoadBalancer LoadBalancer                    = &DefaultLoadBalancer{}
)

type callInfoType struct {
	Timeout     time.Duration
	HttpVersion int
	Token       string
	SSL         bool
}

func IsServer() bool { appLock.RLock(); defer appLock.RUnlock(); return isServer }
func IsClient() bool { appLock.RLock(); defer appLock.RUnlock(); return isClient }

func logError(error string, extra ...any) {
	_logger.Error("Discover: "+error, append(extra, "app", Config.App, "addr", myAddr)...)
}

func logInfo(info string, extra ...any) {
	_logger.Info("Discover: "+info, append(extra, "app", Config.App, "addr", myAddr)...)
}

func SetLogger(logger *log.Logger) {
	_logger = logger
}

func Init() {
	appLock.Lock()
	defer appLock.Unlock()
	if _inited {
		return
	}
	_inited = true
	_ = config.Load(&Config, "discover")

	if Config.CallRetryTimes <= 0 {
		Config.CallRetryTimes = 10
	}
	if Config.Weight <= 0 {
		Config.Weight = 100
	}
	if Config.Registry == "" {
		Config.Registry = DefaultRegistry
	}

	_logger = log.New(id.MakeID(12))
}

// Start launches recovery workers; success means started, not registered.
// Initial startup does not perform Redis network IO on the caller.
// Restart first drains the previous run using bounded recovery IO.
func Start(addr string) bool {
	lifecycleLock.Lock()
	defer lifecycleLock.Unlock()
	Init()
	stopRun()
	calls := getCalls()
	for app, conf := range calls {
		addApp(app, conf, false)
	}
	appLock.Lock()
	myAddr, activeApp, registered = addr, Config.App, false
	isServer = Config.App != "" && Config.Weight > 0 && addr != ""
	serverRedisPool = recoveryPool(Config.Registry, _logger)
	clientRedisPool = serverRedisPool
	pubsubRedisPool = recoveryPool(Config.Registry, _logger)
	heartbeatStop, heartbeatDone = make(chan struct{}), make(chan struct{})
	reconcileWake = make(chan struct{}, 1)
	stop, done, wake := heartbeatStop, heartbeatDone, reconcileWake
	pool, subscriptions := serverRedisPool, pubsubRedisPool
	app, weight, server, logger := activeApp, Config.Weight, isServer, _logger
	isClient = len(calls) > 0
	appLock.Unlock()
	// Subscription callbacks only wake the reconciler; they never perform network IO.
	for name := range calls {
		subscribeApp(subscriptions, name)
	}
	if len(calls) > 0 {
		subscriptions.StartAsync()
	}
	go func() {
		defer close(done)
		var workers sync.WaitGroup
		workers.Add(1)
		go func() { defer workers.Done(); reconcile(stop, wake, pool, logger) }()
		if server {
			workers.Add(1)
			go func() { defer workers.Done(); daemon(stop, pool, app, addr, weight, logger) }()
		}
		workers.Wait()
	}()
	return true
}

func recoveryPool(registry string, logger *log.Logger) *redis.Redis {
	source := redis.GetRedis(registry, logger)
	conf := *source.Config
	// Private pools retain the configured auth/DB but bound recovery IO independently.
	conf.ConnectTimeout, conf.ReadTimeout, conf.WriteTimeout = 500*time.Millisecond, 500*time.Millisecond, 500*time.Millisecond
	quiet := logger.New(logger.GetTraceId())
	quiet.SetLevel(log.CLOSE)
	return redis.NewRedis(&conf, quiet)
}

func wakeReconcile() {
	appLock.RLock()
	wake := reconcileWake
	appLock.RUnlock()
	if wake != nil {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func reconcile(stop <-chan struct{}, wake <-chan struct{}, pool *redis.Redis, logger *log.Logger) {
	var lastFailure time.Time
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		for app := range getCalls() {
			select {
			case <-stop:
				return
			default:
			}
			if err := fetchApp(pool, app); err != nil && time.Since(lastFailure) >= 30*time.Second {
				logger.Warning("Discover: snapshot unavailable; retaining nodes", "app", app, "error", err.Error())
				lastFailure = time.Now()
			}
		}
		select {
		case <-stop:
			return
		case <-wake:
		case <-ticker.C:
		}
	}
}

// IsRegistered reports the last confirmed registration/heartbeat state.
func IsRegistered() bool {
	appLock.RLock()
	defer appLock.RUnlock()
	return registered
}

// Registration is one Redis command: partial failures are retried as a whole.
const registrationScript = `
local old = redis.call('HGET', KEYS[1], ARGV[1])
local live = redis.call('EXISTS', KEYS[2])
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('SET', KEYS[2], '1', 'EX', 10)
if old ~= ARGV[2] or live == 0 or ARGV[3] == '1' then
 redis.call('PUBLISH', 'CH_' .. KEYS[1], ARGV[1] .. ' ' .. ARGV[2])
end
return 1`

// stopRun is called with lifecycleLock held. Wait for workers before removing keys.
func stopRun() {
	appLock.Lock()
	stop, done, subscriptions := heartbeatStop, heartbeatDone, pubsubRedisPool
	if stop != nil {
		close(stop)
		heartbeatStop = nil
	}
	appLock.Unlock()
	if subscriptions != nil {
		subscriptions.Stop()
	}
	if done != nil {
		<-done
	}
	appLock.Lock()
	server, pool, app, addr := isServer, serverRedisPool, activeApp, myAddr
	isServer, isClient, registered = false, false, false
	reconcileWake = nil
	appLock.Unlock()
	if server && pool != nil {
		pool.Do("EVAL", `redis.call('HDEL',KEYS[1],ARGV[1]); redis.call('DEL',KEYS[2]); redis.call('PUBLISH','CH_'..KEYS[1],ARGV[1]..' 0'); return 1`, 2, app, app+"_"+addr, addr)
	}
	if pool != nil {
		_ = pool.GetPool().Close()
	}
	if subscriptions != nil {
		_ = subscriptions.GetPool().Close()
	}
}

func daemon(stop <-chan struct{}, pool *redis.Redis, app, addr string, weight int, logger *log.Logger) {
	delay := time.Second
	var lastFailure time.Time
	for {
		select {
		case <-stop:
			return
		default:
		}
		wasRegistered := IsRegistered()
		force := "0"
		if !wasRegistered {
			force = "1"
		}
		result := pool.Do("EVAL", registrationScript, 2, app, app+"_"+addr, addr, weight, force)
		ready := result.Error == nil
		appLock.Lock()
		registered = ready
		appLock.Unlock()
		if ready && !wasRegistered {
			logger.Info("Discover: registered", "app", app, "addr", addr)
		}
		if !ready && (wasRegistered || time.Since(lastFailure) >= 30*time.Second) {
			logger.Warning("Discover: registration unavailable; recovering", "app", app, "addr", addr, "error", result.Error.Error())
			lastFailure = time.Now()
		}
		if ready {
			delay = time.Second
		} else if delay < 4*time.Second {
			delay *= 2
		}
		waitDelay := delay
		if !ready {
			waitDelay -= time.Duration(rand.Int(0, 250)) * time.Millisecond
		}
		wait := time.NewTimer(waitDelay)
		select {
		case <-stop:
			wait.Stop()
			return
		case <-wait.C:
		}
	}
}

func subscribeApp(pool *redis.Redis, app string) {
	pool.Subscribe("CH_"+app, wakeReconcile, func(_ []byte) { wakeReconcile() })
}

func Stop() {
	lifecycleLock.Lock()
	defer lifecycleLock.Unlock()
	stopRun()
}

func Wait() {
	appLock.RLock()
	done := heartbeatDone
	appLock.RUnlock()
	if done != nil {
		<-done
	}
}

func EasyStart() (string, int) {
	Init()
	port := 0
	if listen := os.Getenv("DISCOVER_LISTEN"); listen != "" {
		if _, p, err := net.SplitHostPort(listen); err == nil {
			port = cast.Int(p)
		} else {
			port = cast.Int(listen)
		}
	}

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		logError("failed to listen", "err", err)
		return "", 0
	}
	addrInfo := ln.Addr().(*net.TCPAddr)
	_ = ln.Close()
	port = addrInfo.Port

	ip := addrInfo.IP
	if !ip.IsGlobalUnicast() {
		addrs, _ := net.InterfaceAddrs()
		for _, a := range addrs {
			if an, ok := a.(*net.IPNet); ok {
				ip4 := an.IP.To4()
				if ip4 == nil || !ip4.IsGlobalUnicast() {
					continue
				}
				if Config.IpPrefix != "" && strings.HasPrefix(ip4.String(), Config.IpPrefix) {
					ip = ip4
					break
				}
				if !strings.HasPrefix(ip4.String(), "172.17.") {
					ip = ip4
				}
			}
		}
	}

	addr := fmt.Sprintf("%s:%d", ip.String(), port)
	if !Start(addr) {
		return "", 0
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		Stop()
	}()

	return ip.String(), port
}

// AddExternalApp updates desired subscriptions without waiting for Redis.
func AddExternalApp(app, callConf string) bool {
	lifecycleLock.Lock()
	defer lifecycleLock.Unlock()
	changed := addApp(app, callConf, false)
	appLock.RLock()
	pool := pubsubRedisPool
	running := heartbeatStop != nil
	appLock.RUnlock()
	if running && pool != nil {
		subscribeApp(pool, app)
		pool.StartAsync()
		appLock.Lock()
		isClient = true
		appLock.Unlock()
	}
	wakeReconcile()
	return changed
}

func SetNode(app, addr string, weight int) {
	pushNode(app, addr, weight)
}

func getCallInfo(app string) *callInfoType {
	appLock.RLock()
	defer appLock.RUnlock()
	return _calls[app]
}

var numberMatcher = regexp.MustCompile(`^\d+(s|ms|us|µs|ns?)?$`)

func addApp(app, callConf string, fetch bool) bool {
	appLock.Lock()
	if Config.Calls == nil {
		Config.Calls = make(map[string]string)
	}
	if Config.Calls[app] == callConf && _appNodes[app] != nil {
		appLock.Unlock()
		return false
	}
	Config.Calls[app] = callConf

	callInfo := &callInfoType{
		Timeout:     10 * time.Second,
		HttpVersion: 2,
		SSL:         false,
	}

	for _, v := range cast.Split(callConf, ":") {
		switch v {
		case "1":
			callInfo.HttpVersion = 1
		case "2":
			callInfo.HttpVersion = 2
		case "s", "https":
			callInfo.SSL = true
			callInfo.HttpVersion = 2
		case "http":
			callInfo.SSL = false
			callInfo.HttpVersion = 1
		case "h2c":
			callInfo.SSL = false
			callInfo.HttpVersion = 2
		default:
			if numberMatcher.MatchString(v) {
				callInfo.Timeout = cast.Duration(v)
			} else {
				callInfo.Token = v
			}
		}
	}

	_calls[app] = callInfo
	if _appNodes[app] == nil {
		_appNodes[app] = make(map[string]*NodeInfo)
	}
	appSubscribed[app] = true
	appLock.Unlock()

	if fetch {
		wakeReconcile()
	}
	return true
}

// A failed snapshot never clears last-known nodes. The script observes membership
// and leases atomically, and never deletes a live registration.
func fetchApp(pool *redis.Redis, app string) error {
	result := pool.Do("EVAL", `local out={}; local nodes=redis.call('HGETALL',KEYS[1]); for i=1,#nodes,2 do if redis.call('EXISTS',KEYS[1]..'_'..nodes[i])==1 then table.insert(out,nodes[i]);table.insert(out,nodes[i+1]);else redis.call('HDEL',KEYS[1],nodes[i]);end;end;return out`, 1, app)
	if result.Error != nil {
		return result.Error
	}
	values := result.Strings()
	live := make(map[string]int, len(values)/2)
	for index := 0; index+1 < len(values); index += 2 {
		live[values[index]] = cast.Int(values[index+1])
	}
	for addr := range getAppNodes(app) {
		if _, ok := live[addr]; !ok {
			pushNode(app, addr, 0)
		}
	}
	for addr, weight := range live {
		pushNode(app, addr, weight)
	}
	return nil
}

func getAppNodes(app string) map[string]*NodeInfo {
	appLock.RLock()
	defer appLock.RUnlock()
	if _appNodes[app] == nil {
		return nil
	}
	nodes := make(map[string]*NodeInfo)
	for k, v := range _appNodes[app] {
		nodes[k] = v
	}
	return nodes
}

func getCalls() map[string]string {
	appLock.RLock()
	defer appLock.RUnlock()
	calls := make(map[string]string)
	for k, v := range Config.Calls {
		calls[k] = v
	}
	return calls
}

func GetAppNodes(app string) map[string]*NodeInfo {
	return getAppNodes(app)
}

func pushNode(app, addr string, weight int) {
	appLock.Lock()
	defer appLock.Unlock()

	if weight <= 0 {
		if _appNodes[app] != nil {
			delete(_appNodes[app], addr)
		}
		return
	}

	if _appNodes[app] == nil {
		_appNodes[app] = make(map[string]*NodeInfo)
	}

	if node, ok := _appNodes[app][addr]; ok {
		node.stateLock.Lock()
		if node.Weight != weight {
			node.UsedTimes = uint64(float64(node.UsedTimes) / float64(node.Weight) * float64(weight))
			node.Weight = weight
		}
		node.stateLock.Unlock()
	} else {
		var avgUsed uint64 = 0
		if len(_appNodes[app]) > 0 {
			var totalScore float64
			for _, n := range _appNodes[app] {
				nodeWeight, used, _ := n.State()
				if nodeWeight > 0 {
					totalScore += float64(used) / float64(nodeWeight)
				}
			}
			avgUsed = uint64(totalScore / float64(len(_appNodes[app])) * float64(weight))
		}
		_appNodes[app][addr] = &NodeInfo{
			Addr:      addr,
			Weight:    weight,
			UsedTimes: avgUsed,
		}
	}
}
