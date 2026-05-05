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

// Response 在默认负载均衡器中不再执行写操作，减少锁竞争
func (lb *DefaultLoadBalancer) Response(appClient *AppClient, node *NodeInfo, err error, response *http.Response, responseTime time.Duration) {
}

// Next 根据得分（UsedTimes / Weight）选择得分最小的节点
func (lb *DefaultLoadBalancer) Next(appClient *AppClient, nodes []*NodeInfo, request *http.Request) *NodeInfo {
	var minScore float64 = -1
	var minNode *NodeInfo
	for _, node := range nodes {
		// 动态计算得分，避免使用 sync.Map 存储，减少内存分配和锁竞争
		score := float64(node.UsedTimes.Load()) / float64(node.Weight)
		if minNode == nil || score < minScore {
			minScore = score
			minNode = node
		}
	}
	return minNode
}
