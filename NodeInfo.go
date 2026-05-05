package discover

import (
	"sync"
)

// NodeInfo 存储服务节点信息
type NodeInfo struct {
	Addr        string   // 节点地址
	Weight      int      // 节点权重
	UsedTimes   uint64   // 已使用次数
	FailedTimes int      // 失败次数
	Data        sync.Map // 运行时自定义数据
}
