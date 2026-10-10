package discover

import (
	"net/http"
	"time"
)

// SetLoadBalancer 设置全局负载均衡策略
func SetLoadBalancer(lb LoadBalancer) {
	settedLoadBalancer = lb
}

// LoadBalancer 负载均衡接口
type LoadBalancer interface {
	// Response 在每个请求完成后调用，用于更新节点状态
	Response(appClient *AppClient, node *NodeInfo, err error, response *http.Response, responseTime time.Duration)

	// Next 根据当前可用节点选择一个最优节点
	Next(appClient *AppClient, nodes []*NodeInfo, request *http.Request) *NodeInfo
}

// DefaultLoadBalancer 默认负载均衡器（简单权重轮询/得分最小者优先）
type DefaultLoadBalancer struct{}

func (lb *DefaultLoadBalancer) Response(appClient *AppClient, node *NodeInfo, err error, response *http.Response, responseTime time.Duration) {
	weight, used, _ := node.State()
	if weight > 0 {
		node.Data.Store("score", float64(used)/float64(weight))
	}
}

func (lb *DefaultLoadBalancer) Next(appClient *AppClient, nodes []*NodeInfo, request *http.Request) *NodeInfo {
	var minScore float64 = -1
	var minNode *NodeInfo
	for _, node := range nodes {
		weight, used, _ := node.State()
		if weight <= 0 {
			continue
		}
		score := float64(used) / float64(weight)
		if minNode == nil || score < minScore {
			minScore = score
			minNode = node
		}
	}
	return minNode
}
