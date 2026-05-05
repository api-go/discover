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

// Discoverer 发现服务实例
type Discoverer struct {
	Config ConfigStruct

	serverRedisPool *redis.Redis
	clientRedisPool *redis.Redis
	pubsubRedisPool *redis.Redis
	isServer        bool
	isClient        bool
	daemonRunning   bool
	myAddr          string
	logger          *log.Logger
	inited          bool

	daemonStopChan chan bool
	appLock        sync.RWMutex
	calls          map[string]*callInfoType
	appNodes       map[string]map[string]*NodeInfo
	appSubscribed  map[string]bool

	settedRoute        func(*AppClient, *http.Request)
	settedLoadBalancer LoadBalancer
}

type callInfoType struct {
	Timeout     time.Duration
	HttpVersion int
	Token       string
	SSL         bool
}

// DefaultDiscoverer 默认的全局发现服务实例
var DefaultDiscoverer = NewDiscoverer()

// NewDiscoverer 创建一个新的发现服务实例
func NewDiscoverer() *Discoverer {
	return &Discoverer{
		Config: ConfigStruct{
			Weight:         100,
			CallRetryTimes: 10,
		},
		logger:             log.DefaultLogger,
		calls:              make(map[string]*callInfoType),
		appNodes:           make(map[string]map[string]*NodeInfo),
		appSubscribed:      make(map[string]bool),
		settedLoadBalancer: &DefaultLoadBalancer{},
	}
}

// IsServer 返回当前节点是否作为服务端运行
func (d *Discoverer) IsServer() bool { return d.isServer }

// IsClient 返回当前节点是否作为客户端运行
func (d *Discoverer) IsClient() bool { return d.isClient }

func (d *Discoverer) logError(msg string, extra ...any) {
	d.logger.Error("Discover: "+msg, append(extra, "app", d.Config.App, "addr", d.myAddr)...)
}

func (d *Discoverer) logInfo(msg string, extra ...any) {
	d.logger.Info("Discover: "+msg, append(extra, "app", d.Config.App, "addr", d.myAddr)...)
}

// SetLogger 设置 Discover 使用的全局 Logger
func (d *Discoverer) SetLogger(logger *log.Logger) {
	d.logger = logger
}

// Init 初始化 Discover 配置
func (d *Discoverer) Init() {
	d.appLock.Lock()
	defer d.appLock.Unlock()
	if d.inited {
		return
	}
	d.inited = true
	// 如果是默认实例，尝试加载配置
	if d == DefaultDiscoverer {
		_ = config.Load(&d.Config, "discover")
		Config = d.Config // 保持 Config 变量同步
	}

	if d.Config.CallRetryTimes <= 0 {
		d.Config.CallRetryTimes = 10
	}
	if d.Config.Weight <= 0 {
		d.Config.Weight = 100
	}
	if d.Config.Registry == "" {
		d.Config.Registry = DefaultRegistry
	}

	if d.logger == log.DefaultLogger || d.logger == nil {
		d.logger = log.New(id.MakeID(12))
	}
}

// Start 启动服务发现，指定当前节点的外部访问地址
func (d *Discoverer) Start(addr string) bool {
	d.Init()
	d.myAddr = addr

	d.isServer = d.Config.App != "" && d.Config.Weight > 0
	if d.isServer && d.Config.Registry != "" {
		d.serverRedisPool = redis.GetRedis(d.Config.Registry, d.logger)
		if d.serverRedisPool.Error != nil {
			d.logError(d.serverRedisPool.Error.Error())
		}

		// 注册节点
		if d.serverRedisPool.Do("HSET", d.Config.App, addr, d.Config.Weight).Error == nil {
			d.serverRedisPool.Do("SETEX", d.Config.App+"_"+addr, 10, "1")
			d.logInfo("registered")
			d.serverRedisPool.PUBLISH("CH_"+d.Config.App, fmt.Sprintf("%s %d", addr, d.Config.Weight))
			d.daemonRunning = true
			d.daemonStopChan = make(chan bool)
			go d.daemon()
		} else {
			d.logError("register failed")
		}
	}

	calls := d.getCalls()
	if len(calls) > 0 {
		for app, conf := range calls {
			d.addApp(app, conf, false)
		}
		if !d.startSub() {
			return false
		}
	}
	return true
}

func (d *Discoverer) daemon() {
	d.logInfo("daemon thread started")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for d.daemonRunning {
		<-ticker.C
		if !d.daemonRunning {
			break
		}

		if d.isServer && d.serverRedisPool != nil {
			if !d.serverRedisPool.Do("HEXISTS", d.Config.App, d.myAddr).Bool() {
				d.logInfo("lost app registered info, re-registering")
				if d.serverRedisPool.Do("HSET", d.Config.App, d.myAddr, d.Config.Weight).Error == nil {
					d.serverRedisPool.Do("SETEX", d.Config.App+"_"+d.myAddr, 10, "1")
					d.serverRedisPool.PUBLISH("CH_"+d.Config.App, fmt.Sprintf("%s %d", d.myAddr, d.Config.Weight))
				}
			} else {
				d.serverRedisPool.Do("SETEX", d.Config.App+"_"+d.myAddr, 10, "1")
			}
		}
	}
	d.logInfo("daemon thread stopped")
	if d.daemonStopChan != nil {
		d.daemonStopChan <- true
	}
}

func (d *Discoverer) startSub() bool {
	if d.Config.Registry == "" {
		return true
	}

	d.appLock.Lock()
	if d.clientRedisPool == nil {
		d.clientRedisPool = redis.GetRedis(d.Config.Registry, d.logger)
	}

	if d.pubsubRedisPool == nil {
		d.pubsubRedisPool = redis.GetRedis(d.Config.Registry, d.logger.New(id.MakeID(12)))
		// 订阅所有已注册的应用
		for app := range d.appSubscribed {
			d.subscribeAppUnderLock(app)
		}
		// 必须在释放锁之前完成配置，但在释放锁之后启动，避免死锁
		d.appLock.Unlock()
		d.pubsubRedisPool.Start()
		d.appLock.Lock()
	}

	d.isClient = true
	d.appLock.Unlock()
	return true
}

func (d *Discoverer) subscribeAppUnderLock(app string) {
	d.pubsubRedisPool.Subscribe("CH_"+app, func() {
		d.fetchApp(app)
	}, func(data []byte) {
		a := strings.Split(string(data), " ")
		addr := a[0]
		weight := 0
		if len(a) == 2 {
			weight = cast.Int(a[1])
		}
		d.logInfo("received node update", "app", app, "addr", addr, "weight", weight)
		d.pushNode(app, addr, weight)
	})
}

// Stop 停止 Discover 并从注册中心注销当前节点
func (d *Discoverer) Stop() {
	d.appLock.Lock()
	if d.isClient && d.pubsubRedisPool != nil {
		d.pubsubRedisPool.Stop()
		d.isClient = false
	}

	if d.isServer {
		d.daemonRunning = false
		if d.serverRedisPool != nil {
			d.serverRedisPool.Do("HDEL", d.Config.App, d.myAddr)
			d.serverRedisPool.Do("DEL", d.Config.App+"_"+d.myAddr)
			d.serverRedisPool.PUBLISH("CH_"+d.Config.App, fmt.Sprintf("%s %d", d.myAddr, 0))
		}
		d.isServer = false
	}
	d.appLock.Unlock()
}

// Wait 等待守护进程退出
func (d *Discoverer) Wait() {
	if d.daemonStopChan != nil {
		<-d.daemonStopChan
		d.daemonStopChan = nil
	}
}

// EasyStart 自动根据环境变量和本地网卡信息启动 Discover
func (d *Discoverer) EasyStart() (string, int) {
	d.Init()
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
		d.logError("failed to listen", "err", err)
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
				if d.Config.IpPrefix != "" && strings.HasPrefix(ip4.String(), d.Config.IpPrefix) {
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
	if !d.Start(addr) {
		return "", 0
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		d.Stop()
	}()

	return ip.String(), port
}

// AddExternalApp 动态添加需要发现的外部应用
func (d *Discoverer) AddExternalApp(app, callConf string) bool {
	if d.addApp(app, callConf, true) {
		if !d.isClient {
			d.startSub()
		} else {
			d.appLock.Lock()
			d.subscribeAppUnderLock(app)
			d.appLock.Unlock()
			d.fetchApp(app) // 同步拉取一次
		}
		return true
	}
	return false
}

// SetNode 手动设置某个服务的节点信息
func (d *Discoverer) SetNode(app, addr string, weight int) {
	d.pushNode(app, addr, weight)
}

func (d *Discoverer) getCallInfo(app string) *callInfoType {
	d.appLock.RLock()
	defer d.appLock.RUnlock()
	return d.calls[app]
}

var numberMatcher = regexp.MustCompile(`^\d+(s|ms|us|µs|ns?)?$`)

func (d *Discoverer) addApp(app, callConf string, fetch bool) bool {
	d.appLock.Lock()
	if d.Config.Calls == nil {
		d.Config.Calls = make(map[string]string)
	}
	if d.Config.Calls[app] == callConf && d.appNodes[app] != nil {
		d.appLock.Unlock()
		return false
	}
	d.Config.Calls[app] = callConf

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

	d.calls[app] = callInfo
	if d.appNodes[app] == nil {
		d.appNodes[app] = make(map[string]*NodeInfo)
	}
	d.appSubscribed[app] = true
	d.appLock.Unlock()

	if fetch && d.isClient {
		d.fetchApp(app)
	}
	return true
}

func (d *Discoverer) fetchApp(app string) {
	d.appLock.RLock()
	pool := d.clientRedisPool
	d.appLock.RUnlock()
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

	currentNodes := d.getAppNodes(app)
	if currentNodes != nil {
		for addr := range currentNodes {
			if _, ok := results[addr]; !ok {
				d.pushNode(app, addr, 0)
			}
		}
	}

	for addr, res := range results {
		d.pushNode(app, addr, res.Int())
	}
}

func (d *Discoverer) getAppNodes(app string) map[string]*NodeInfo {
	d.appLock.RLock()
	defer d.appLock.RUnlock()
	if d.appNodes[app] == nil {
		return nil
	}
	nodes := make(map[string]*NodeInfo)
	for k, v := range d.appNodes[app] {
		nodes[k] = v
	}
	return nodes
}

func (d *Discoverer) getCalls() map[string]string {
	d.appLock.RLock()
	defer d.appLock.RUnlock()
	calls := make(map[string]string)
	for k, v := range d.Config.Calls {
		calls[k] = v
	}
	return calls
}

// GetAppNodes 获取某个应用的所有节点列表
func (d *Discoverer) GetAppNodes(app string) map[string]*NodeInfo {
	return d.getAppNodes(app)
}

func (d *Discoverer) pushNode(app, addr string, weight int) {
	d.appLock.Lock()
	defer d.appLock.Unlock()

	if weight <= 0 {
		if d.appNodes[app] != nil {
			delete(d.appNodes[app], addr)
		}
		return
	}

	if d.appNodes[app] == nil {
		d.appNodes[app] = make(map[string]*NodeInfo)
	}

	if node, ok := d.appNodes[app][addr]; ok {
		if node.Weight != weight {
			used := node.UsedTimes.Load()
			node.UsedTimes.Store(uint64(float64(used) / float64(node.Weight) * float64(weight)))
			node.Weight = weight
		}
	} else {
		var avgUsed uint64 = 0
		if len(d.appNodes[app]) > 0 {
			var totalScore float64
			for _, n := range d.appNodes[app] {
				totalScore += float64(n.UsedTimes.Load()) / float64(n.Weight)
			}
			avgUsed = uint64(totalScore / float64(len(d.appNodes[app])) * float64(weight))
		}
		node := &NodeInfo{
			Addr:   addr,
			Weight: weight,
		}
		node.UsedTimes.Store(avgUsed)
		d.appNodes[app][addr] = node
	}
}

// 以下是包级别 API，通过转发给 DefaultDiscoverer 实现兼容性

func IsServer() bool { return DefaultDiscoverer.IsServer() }
func IsClient() bool { return DefaultDiscoverer.IsClient() }

func logError(msg string, extra ...any) {
	DefaultDiscoverer.logError(msg, extra...)
}

func logInfo(msg string, extra ...any) {
	DefaultDiscoverer.logInfo(msg, extra...)
}

func SetLogger(logger *log.Logger) {
	DefaultDiscoverer.SetLogger(logger)
}

func Init() {
	DefaultDiscoverer.Init()
}

func Start(addr string) bool {
	return DefaultDiscoverer.Start(addr)
}

func Stop() {
	DefaultDiscoverer.Stop()
}

func Wait() {
	DefaultDiscoverer.Wait()
}

func EasyStart() (string, int) {
	return DefaultDiscoverer.EasyStart()
}

func AddExternalApp(app, callConf string) bool {
	return DefaultDiscoverer.AddExternalApp(app, callConf)
}

func SetNode(app, addr string, weight int) {
	DefaultDiscoverer.SetNode(app, addr, weight)
}

func GetAppNodes(app string) map[string]*NodeInfo {
	return DefaultDiscoverer.GetAppNodes(app)
}
