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

	"apigo.cc/go/cast"
	"apigo.cc/go/config"
	"apigo.cc/go/id"
	"apigo.cc/go/log"
	"apigo.cc/go/redis"
)

var (
	serverRedisPool *redis.Redis
	clientRedisPool *redis.Redis
	pubsubRedisPool *redis.Redis
	isServer        = false
	isClient        = false
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

func Start(addr string) bool {
	lifecycleLock.Lock()
	defer lifecycleLock.Unlock()
	Init()
	stopHeartbeat()
	appLock.Lock()
	myAddr = addr

	isServer = Config.App != "" && Config.Weight > 0 && addr != ""
	if isServer && Config.Registry != "" {
		serverRedisPool = redis.GetRedis(Config.Registry, _logger)
		if serverRedisPool.Error != nil {
			logError(serverRedisPool.Error.Error())
		}

		// 注册节点
		if serverRedisPool.HSET(Config.App, addr, Config.Weight) {
			serverRedisPool.SETEX(Config.App+"_"+addr, 10, "1")
			logInfo("registered")
			serverRedisPool.PUBLISH("CH_"+Config.App, fmt.Sprintf("%s %d", addr, Config.Weight))
			heartbeatStop = make(chan struct{})
			heartbeatDone = make(chan struct{})
			go daemon(heartbeatStop, heartbeatDone, serverRedisPool, Config.App, addr, Config.Weight, _logger)
		} else {
			logError("register failed")
		}
	}

	appLock.Unlock()
	calls := getCalls()
	if len(calls) > 0 {
		for app, conf := range calls {
			addApp(app, conf, false)
		}
		if !startSub() {
			return false
		}
	}
	return true
}

// stopHeartbeat is called with lifecycleLock held; channel close wakes the loop immediately.
func stopHeartbeat() {
	appLock.Lock()
	if heartbeatStop != nil {
		close(heartbeatStop)
		heartbeatStop = nil
	}
	done := heartbeatDone
	appLock.Unlock()
	if done != nil {
		<-done
	}
}

func daemon(stop <-chan struct{}, done chan struct{}, pool *redis.Redis, app, addr string, weight int, logger *log.Logger) {
	defer close(done)
	logger.Info("Discover: daemon thread started", "app", app, "addr", addr)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			logger.Info("Discover: daemon thread stopped", "app", app, "addr", addr)
			return
		case <-ticker.C:
			// A stop concurrent with the tick must be observed before another heartbeat.
			select {
			case <-stop:
				return
			default:
			}
			if !pool.HEXISTS(app, addr) {
				logger.Info("Discover: lost app registered info, re-registering", "app", app, "addr", addr)
				if pool.HSET(app, addr, weight) {
					pool.SETEX(app+"_"+addr, 10, "1")
					pool.PUBLISH("CH_"+app, fmt.Sprintf("%s %d", addr, weight))
				}
			} else {
				pool.SETEX(app+"_"+addr, 10, "1")
			}
		}
	}
}

func startSub() bool {
	if Config.Registry == "" {
		return true
	}

	appLock.Lock()
	if clientRedisPool == nil {
		clientRedisPool = redis.GetRedis(Config.Registry, _logger)
	}

	if pubsubRedisPool == nil {
		pubsubRedisPool = redis.GetRedis(Config.Registry, _logger.New(id.MakeID(12)))
	}
	if clientRedisPool.Error != nil || pubsubRedisPool.Error != nil {
		appLock.Unlock()
		return false
	}
	// Redis reset callbacks call fetchApp and acquire appLock. Never call
	// Subscribe or Start while holding appLock: Redis holds its subscription
	// lock while invoking these callbacks.
	pool := pubsubRedisPool
	apps := make([]string, 0, len(appSubscribed))
	for app := range appSubscribed {
		apps = append(apps, app)
	}
	appLock.Unlock()
	for _, app := range apps {
		subscribeApp(pool, app)
	}
	pool.Start()
	appLock.Lock()

	isClient = true
	appLock.Unlock()
	return true
}

func subscribeApp(pool *redis.Redis, app string) {
	pool.Subscribe("CH_"+app, func() {
		fetchApp(app)
	}, func(data []byte) {
		a := strings.Split(string(data), " ")
		addr := a[0]
		weight := 0
		if len(a) == 2 {
			weight = cast.Int(a[1])
		}
		logInfo("received node update", "app", app, "addr", addr, "weight", weight)
		pushNode(app, addr, weight)
	})
}

func Stop() {
	lifecycleLock.Lock()
	defer lifecycleLock.Unlock()
	stopHeartbeat()
	appLock.Lock()
	client, subscriptions := isClient, pubsubRedisPool
	server, registry, app, addr := isServer, serverRedisPool, Config.App, myAddr
	isClient, isServer = false, false
	appLock.Unlock()
	if client && subscriptions != nil {
		subscriptions.Stop()
	}
	if server && registry != nil {
		registry.HDEL(app, addr)
		registry.DEL(app + "_" + addr)
		registry.PUBLISH("CH_"+app, fmt.Sprintf("%s %d", addr, 0))
	}
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

func AddExternalApp(app, callConf string) bool {
	lifecycleLock.Lock()
	defer lifecycleLock.Unlock()
	if addApp(app, callConf, true) {
		appLock.RLock()
		client, pool := isClient, pubsubRedisPool
		appLock.RUnlock()
		if !client || pool == nil {
			startSub()
		} else {
			subscribeApp(pool, app)
		}
		return true
	}
	return false
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
		fetchApp(app)
	}
	return true
}

func fetchApp(app string) {
	appLock.RLock()
	pool := clientRedisPool
	appLock.RUnlock()
	if pool == nil {
		return
	}

	results := pool.HGETALL(app)

	// 检查存活
	for addr := range results {
		if !pool.EXISTS(app + "_" + addr) {
			pool.HDEL(app, addr)
			delete(results, addr)
		}
	}

	currentNodes := getAppNodes(app)
	if currentNodes != nil {
		for addr := range currentNodes {
			if _, ok := results[addr]; !ok {
				pushNode(app, addr, 0)
			}
		}
	}

	for addr, res := range results {
		pushNode(app, addr, res.Int())
	}
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
