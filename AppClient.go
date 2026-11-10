package discover

import (
	"net/http"

	"github.com/api-go/log"
)

// AppClient 用于管理单个请求的重试和负载均衡状态
type AppClient struct {
	excludes map[string]bool
	tryTimes int
	Logger   *log.Logger
	App      string
	Method   string
	Path     string
	Data     *map[string]any
	Headers  *map[string]string
}

func (ac *AppClient) logError(error string, extra ...any) {
	if ac.Logger == nil {
		ac.Logger = log.DefaultLogger
	}
	ac.Logger.Error("Discover Client: "+error, extra...)
}

func (ac *AppClient) Next(app string, request *http.Request) *NodeInfo {
	return ac.NextWithNode(app, "", request)
}

func (ac *AppClient) CheckApp(app string) bool {
	nodes := getAppNodes(app)
	if nodes == nil {
		if !addApp(app, "", true) {
			ac.logError("app not found", "app", app, "calls", Config.Calls)
			return false
		}
	}
	return true
}

func (ac *AppClient) NextWithNode(app, withNode string, request *http.Request) *NodeInfo {
	if ac.excludes == nil {
		ac.excludes = make(map[string]bool)
	}

	allNodes := getAppNodes(app)
	if len(allNodes) == 0 {
		ac.logError("node not found", "app", app)
		return nil
	}

	ac.tryTimes++
	if withNode != "" {
		if ac.excludes[withNode] {
			return nil
		}
		ac.excludes[withNode] = true
		return allNodes[withNode]
	}

	readyNodes := make([]*NodeInfo, 0)
	for _, node := range allNodes {
		_, _, failed := node.State()
		if ac.excludes[node.Addr] || failed >= Config.CallRetryTimes {
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
		node = settedLoadBalancer.Next(ac, readyNodes, request)
		if node != nil {
			ac.excludes[node.Addr] = true
		}
	}

	if node == nil {
		ac.logError("no available node", "app", app, "tryTimes", ac.tryTimes)
	}

	return node
}
