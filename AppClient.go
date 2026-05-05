package discover

import (
	"net/http"

	"apigo.cc/go/log"
)

// AppClient 用于管理单个请求的重试和负载均衡状态
type AppClient struct {
	discoverer *Discoverer
	excludes   map[string]bool // 本次请求已排除的节点
	attempts   int             // 本次请求的重试次数
	Logger     *log.Logger     // 用于日志记录的 Logger
	App        string          // 目标应用名称
	Method     string          // 请求方法
	Path       string          // 请求路径
	Data       map[string]any  // 请求数据
	Headers    map[string]string // 请求头
}

// logError 记录 Discover 客户端错误
func (ac *AppClient) logError(msg string, extra ...any) {
	if ac.Logger == nil {
		ac.Logger = log.DefaultLogger
	}
	ac.Logger.Error("Discover Client: "+msg, extra...)
}

// Next 获取下一个可用节点
func (ac *AppClient) Next(app string, request *http.Request) *NodeInfo {
	if ac.discoverer == nil {
		ac.discoverer = DefaultDiscoverer
	}
	return ac.NextWithNode(app, "", request)
}

// CheckApp 检查并尝试添加应用
func (ac *AppClient) CheckApp(app string) bool {
	nodes := ac.discoverer.GetAppNodes(app)
	if nodes == nil {
		conf := ac.discoverer.GetConfig()
		if !ac.discoverer.AddExternalApp(app, "") {
			ac.logError("app not found", "app", app, "calls", conf.Calls)
			return false
		}
	}
	return true
}

// NextWithNode 获取下一个可用节点，支持指定节点
func (ac *AppClient) NextWithNode(app, withNode string, request *http.Request) *NodeInfo {
	if ac.excludes == nil {
		ac.excludes = make(map[string]bool)
	}

	allNodes := ac.discoverer.GetAppNodes(app)
	if len(allNodes) == 0 {
		ac.logError("node not found", "app", app)
		return nil
	}

	ac.attempts++
	if withNode != "" {
		ac.excludes[withNode] = true
		return allNodes[withNode]
	}

	conf := ac.discoverer.GetConfig()
	readyNodes := make([]*NodeInfo, 0, len(allNodes))
	for _, node := range allNodes {
		if ac.excludes[node.Addr] || node.FailedTimes.Load() >= int32(conf.CallRetryTimes) {
			continue
		}
		readyNodes = append(readyNodes, node)
	}

	if len(readyNodes) == 0 {
		// 如果没有可用节点，尝试已经失败但未被本次请求排除的节点
		for _, node := range allNodes {
			if !ac.excludes[node.Addr] {
				readyNodes = append(readyNodes, node)
			}
		}
	}

	var node *NodeInfo
	if len(readyNodes) > 0 {
		node = ac.discoverer.settedLoadBalancer.Next(ac, readyNodes, request)
		if node != nil {
			ac.excludes[node.Addr] = true
		}
	}

	if node == nil {
		ac.logError("no available node", "app", app, "attempts", ac.attempts)
	}

	return node
}
