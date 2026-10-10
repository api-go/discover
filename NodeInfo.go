package discover

import (
	"sync"
)

// NodeInfo 存储服务节点信息
type NodeInfo struct {
	Addr        string // 节点地址
	Weight      int    // 节点权重
	UsedTimes   uint64 // 已使用次数
	FailedTimes int    // 失败次数
	stateLock   sync.Mutex
	Data        sync.Map // 运行时自定义数据
}

// State returns a consistent snapshot of the node counters.
func (n *NodeInfo) State() (weight int, used uint64, failed int) {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()
	return n.Weight, n.UsedTimes, n.FailedTimes
}

func (n *NodeInfo) beginCall() {
	n.stateLock.Lock()
	n.UsedTimes++
	n.stateLock.Unlock()
}

func (n *NodeInfo) finishCall(failed bool) int {
	n.stateLock.Lock()
	defer n.stateLock.Unlock()
	if failed {
		n.FailedTimes++
	} else {
		n.FailedTimes = 0
	}
	return n.FailedTimes
}
