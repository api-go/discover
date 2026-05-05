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
	daemonRunning   = false
	myAddr          = ""
	_logger         = log.DefaultLogger
	_inited         = false

	daemonStopChan chan bool
	appLock        sync.RWMutex
	_calls         = map[string]*callInfoType{}
	_appNodes      = map[string]map[string]*NodeInfo{}
	appSubscribed  = map[string]bool{}

	settedRoute        func(*AppClient, *http.Request) = nil
	settedLoadBalancer LoadBalancer                    = &DefaultLoadBalancer{}
)

type callInfoType struct {
	Timeout     time.Duration
	HttpVersion int
	Token       string
	SSL         bool
}

func IsServer() bool { return isServer }
func IsClient() bool { return isClient }

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
	Init()
	myAddr = addr

	isServer = Config.App != "" && Config.Weight > 0
	if isServer && Config.Registry != "" {
		serverRedisPool = redis.GetRedis(Config.Registry, _logger)
		if serverRedisPool.Error != nil {
			logError(serverRedisPool.Error.Error())
		}

		// 注册节点
		if serverRedisPool.Do("HSET", Config.App, addr, Config.Weight).Error == nil {
			serverRedisPool.Do("SETEX", Config.App+"_"+addr, 10, "1")
			logInfo("registered")
			serverRedisPool.PUBLISH("CH_"+Config.App, fmt.Sprintf("%s %d", addr, Config.Weight))
			daemonRunning = true
			daemonStopChan = make(chan bool)
			go daemon()
		} else {
			logError("register failed")
		}
	}

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

func daemon() {
	logInfo("daemon thread started")
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for daemonRunning {
		<-ticker.C
		if !daemonRunning {
			break
		}

		if isServer && serverRedisPool != nil {
			if !serverRedisPool.Do("HEXISTS", Config.App, myAddr).Bool() {
				logInfo("lost app registered info, re-registering")
				if serverRedisPool.Do("HSET", Config.App, myAddr, Config.Weight).Error == nil {
					serverRedisPool.Do("SETEX", Config.App+"_"+myAddr, 10, "1")
					serverRedisPool.PUBLISH("CH_"+Config.App, fmt.Sprintf("%s %d", myAddr, Config.Weight))
				}
			} else {
				serverRedisPool.Do("SETEX", Config.App+"_"+myAddr, 10, "1")
			}
		}
	}
	logInfo("daemon thread stopped")
	if daemonStopChan != nil {
		daemonStopChan <- true
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
		// 订阅所有已注册的应用
		for app := range appSubscribed {
			subscribeAppUnderLock(app)
		}
		// 必须在释放锁之前完成配置，但在释放锁之后启动，避免死锁
		appLock.Unlock()
		pubsubRedisPool.Start()
		appLock.Lock()
	}

	isClient = true
	appLock.Unlock()
	return true
}

func subscribeAppUnderLock(app string) {
	pubsubRedisPool.Subscribe("CH_"+app, func() {
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
	appLock.Lock()
	if isClient && pubsubRedisPool != nil {
		pubsubRedisPool.Stop()
		isClient = false
	}

	if isServer {
		daemonRunning = false
		if serverRedisPool != nil {
			serverRedisPool.Do("HDEL", Config.App, myAddr)
			serverRedisPool.Do("DEL", Config.App+"_"+myAddr)
			serverRedisPool.PUBLISH("CH_"+Config.App, fmt.Sprintf("%s %d", myAddr, 0))
		}
		isServer = false
	}
	appLock.Unlock()
}

func Wait() {
	if daemonStopChan != nil {
		<-daemonStopChan
		daemonStopChan = nil
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
	if addApp(app, callConf, true) {
		if !isClient {
			startSub()
		} else {
			appLock.Lock()
			subscribeAppUnderLock(app)
			appLock.Unlock()
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

	results := pool.Do("HGETALL", app).ResultMap()
	
	// 检查存活
	for addr := range results {
		if !pool.Do("EXISTS", app+"_"+addr).Bool() {
			pool.Do("HDEL", app, addr)
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
		if node.Weight != weight {
			node.UsedTimes = uint64(float64(node.UsedTimes) / float64(node.Weight) * float64(weight))
			node.Weight = weight
		}
	} else {
		var avgUsed uint64 = 0
		if len(_appNodes[app]) > 0 {
			var totalScore float64
			for _, n := range _appNodes[app] {
				totalScore += float64(n.UsedTimes) / float64(n.Weight)
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
