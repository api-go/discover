package discover

import (
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"apigo.cc/go/cast"
	gohttp "apigo.cc/go/http"
	"apigo.cc/go/id"
	"apigo.cc/go/log"
	"apigo.cc/go/redis"
)

// Discoverer 发现服务实例
type Discoverer struct {
	config Config
	registry string
	app string
	
	serverRedisPool *redis.Redis
	clientRedisPool *redis.Redis
	pubsubRedisPool *redis.Redis
	isServer        bool
	isClient        bool
	daemonRunning      atomic.Bool
	myAddr             string
	logger             *log.Logger
		daemonStopSignal   chan struct{}
	daemonDoneSignal   chan struct{}
	appLock            sync.RWMutex
	calls          map[string]CallConfig
	appNodes       map[string]map[string]*NodeInfo
	appSubscribed  map[string]bool

	appClientPools     map[string]*gohttp.Client
	appClientPoolsLock sync.RWMutex

	settedRoute        func(*AppClient, *http.Request)
	settedLoadBalancer LoadBalancer
}




// IsServer 返回当前节点是否作为服务端运行
func (d *Discoverer) IsServer() bool { return d.isServer }

// IsClient 返回当前节点是否作为客户端运行
func (d *Discoverer) IsClient() bool { return d.isClient }

func (d *Discoverer) logError(msg string, extra ...any) {
	d.logger.Error("Discover: "+msg, append(extra, "app", d.app, "addr", d.myAddr)...)
}

func (d *Discoverer) logInfo(msg string, extra ...any) {
	d.logger.Info("Discover: "+msg, append(extra, "app", d.app, "addr", d.myAddr)...)
}

// SetLogger 设置 Discover 使用的全局 Logger
func (d *Discoverer) SetLogger(logger *log.Logger) {
	d.logger = logger
}

// New 创建一个新的发现服务实例
func New(logger *log.Logger, confs ...Config) *Discoverer {
	var conf Config
	if len(confs) > 0 {
		conf = confs[0]
	}
	if conf.CallRetryTimes <= 0 {
		conf.CallRetryTimes = 10
	}
	if conf.Weight <= 0 {
		conf.Weight = 100
	}

	if logger == nil {
		logger = log.DefaultLogger
	}

	d := &Discoverer{
		config:             conf,
		calls:              make(map[string]CallConfig),
		appNodes:           make(map[string]map[string]*NodeInfo),
		appSubscribed:      make(map[string]bool),
		appClientPools:     make(map[string]*gohttp.Client),
		settedLoadBalancer: &DefaultLoadBalancer{},
		daemonStopSignal:   make(chan struct{}),
		daemonDoneSignal:   make(chan struct{}),
		logger:             logger,
	}
	return d
}

// Start 启动服务发现，指定当前节点的外部访问地址
func Start(registry, app, addr string, logger *log.Logger, confs ...Config) *Discoverer {
	d := New(logger, confs...)
	d.registry = registry
	d.app = app
	d.myAddr = addr

	d.isServer = d.app != "" && d.config.Weight > 0
	if d.isServer && d.registry != "" {
		d.serverRedisPool = redis.GetRedis(d.registry, d.logger)
		if d.serverRedisPool.Error != nil {
			d.logError(d.serverRedisPool.Error.Error())
		}

		// 注册节点
		if d.serverRedisPool.Do("HSET", d.app, addr, d.config.Weight).Error == nil {
			d.serverRedisPool.Do("SETEX", d.app+"_"+addr, 10, "1")
			d.logInfo("registered")
			d.serverRedisPool.PUBLISH("CH_"+d.app, fmt.Sprintf("%s %d", addr, d.config.Weight))
			d.daemonRunning.Store(true)
			d.daemonStopSignal = make(chan struct{})
			d.daemonDoneSignal = make(chan struct{})
			go d.daemon()
		} else {
			d.logError("register failed")
		}
	}

	calls := d.config.Calls
	if len(calls) > 0 {
		for callApp, c := range calls {
			d.addApp(callApp, c, false)
		}
		if !d.startSub() {
			return d
		}
	}
	return d
}

// Open 启动服务发现仅作为客户端
func Open(registry string, logger *log.Logger, confs ...Config) *Discoverer {
	d := New(logger, confs...)
	d.registry = registry
	calls := d.config.Calls
	if len(calls) > 0 {
		for callApp, c := range calls {
			d.addApp(callApp, c, false)
		}
	}
	d.startSub()
	return d
}



func (d *Discoverer) daemon() {
	d.logInfo("daemon thread started")
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for d.daemonRunning.Load() {
		select {
		case <-ticker.C:
			if !d.daemonRunning.Load() {
				break
			}

			if d.isServer && d.serverRedisPool != nil {
				if !d.serverRedisPool.Do("HEXISTS", d.app, d.myAddr).Bool() {
					d.logInfo("lost app registered info, re-registering")
					if d.serverRedisPool.Do("HSET", d.app, d.myAddr, d.config.Weight).Error == nil {
						d.serverRedisPool.Do("SETEX", d.app+"_"+d.myAddr, 10, "1")
						d.serverRedisPool.PUBLISH("CH_"+d.app, fmt.Sprintf("%s %d", d.myAddr, d.config.Weight))
					}
				} else {
					d.serverRedisPool.Do("SETEX", d.app+"_"+d.myAddr, 10, "1")
				}
			}
		case <-d.daemonStopSignal:
			goto done
		}
	}
done:
	d.logInfo("daemon thread stopped")
	close(d.daemonDoneSignal)
}

func (d *Discoverer) startSub() bool {
	if d.registry == "" {
		return true
	}

	d.appLock.Lock()
	if d.clientRedisPool == nil {
		d.clientRedisPool = redis.GetRedis(d.registry, d.logger)
	}

	if d.pubsubRedisPool == nil {
		d.pubsubRedisPool = redis.GetRedis(d.registry, d.logger.New(id.MakeID(12)))
		// 订阅所有已注册的应用
		for app := range d.appSubscribed {
			d.subscribeApp(app)
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

func (d *Discoverer) subscribeApp(app string) {
	if d.pubsubRedisPool == nil {
		d.appSubscribed[app] = true
		return
	}

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

func (d *Discoverer) subscribeAppWithLock(app string) {
	d.appLock.Lock()
	defer d.appLock.Unlock()
	d.subscribeApp(app)
}

// Stop 停止 Discover 并从注册中心注销当前节点
func (d *Discoverer) Stop() {
	d.appLock.Lock()

	// 1. 提取需要的状态，提前修改标志位
	isClient := d.isClient
	pubsub := d.pubsubRedisPool
	d.isClient = false

	isServer := d.isServer
	serverPool := d.serverRedisPool
	myAddr := d.myAddr

	if isServer {
		d.daemonRunning.Store(false)
		if d.daemonStopSignal != nil {
			close(d.daemonStopSignal)
		}
		d.isServer = false
	}

	// 核心修复：在这里尽早释放锁！避免与 pushNode 回调发生死锁
	d.appLock.Unlock()

	// 2. 在无锁状态下进行耗时的网络和停止操作
	if isClient && pubsub != nil {
		pubsub.Stop()
	}

	if isServer && serverPool != nil {
		serverPool.Do("HDEL", d.app, myAddr)
		serverPool.Do("DEL", d.app+"_"+myAddr)
		serverPool.PUBLISH("CH_"+d.app, fmt.Sprintf("%s %d", myAddr, 0))
	}

	// 3. 释放 HTTP 连接池
	d.appClientPoolsLock.Lock()
	for _, client := range d.appClientPools {
		client.Destroy()
	}
	d.appClientPools = make(map[string]*gohttp.Client)
	d.appClientPoolsLock.Unlock()

	// 4. 重置状态以支持重新启动
	d.appLock.Lock()
	d.serverRedisPool = nil
	d.clientRedisPool = nil
	d.pubsubRedisPool = nil
	d.appNodes = make(map[string]map[string]*NodeInfo)
	d.appSubscribed = make(map[string]bool)
	d.appLock.Unlock()
}

// Wait 等待守护进程退出
func (d *Discoverer) Wait() {
	if d.daemonDoneSignal != nil {
		<-d.daemonDoneSignal
	}
}


// AddExternalApp 动态添加需要发现的外部应用
func (d *Discoverer) AddExternalApp(app string, callConf CallConfig) bool {
	if d.addApp(app, callConf, true) {
		if !d.isClient {
			d.startSub()
		} else {
			d.subscribeAppWithLock(app)
		}
		d.fetchApp(app) // 同步拉取一次
		return true
	}
	return false
}

func (d *Discoverer) getCallInfo(app string) (CallConfig, bool) {
	d.appLock.RLock()
	defer d.appLock.RUnlock()
	info, exists := d.calls[app]
	return info, exists
}

func (d *Discoverer) addApp(app string, callConf CallConfig, fetch bool) bool {
	d.appLock.Lock()

	// 1. 写时复制（Copy-on-Write）：创建一个全新的 Map 避免影响读操作
	newCalls := make(map[string]CallConfig)
	for k, v := range d.config.Calls {
		newCalls[k] = v
	}

	if existing, ok := newCalls[app]; ok {
		// compare? simple enough to just overwrite if we want to be safe, but let's check basic equality or just overwrite
		_ = existing
	}
	if d.appNodes[app] != nil {
		// If nodes exist, we might just be updating config
	}

	newCalls[app] = callConf
	d.config.Calls = newCalls // 将新的 Map 赋值给 Config
	d.calls[app] = callConf

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

// SetNode 手动设置某个服务的节点信息
func (d *Discoverer) SetNode(app, addr string, weight int) {
	d.pushNode(app, addr, weight)
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

