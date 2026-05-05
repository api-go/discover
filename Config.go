package discover

import (
	"sync"
)

// ConfigStruct 存储发现服务的配置
type ConfigStruct struct {
	Registry       string            // 注册中心地址，如 redis://:@127.0.0.1:6379/15
	App            string            // 当前应用名称
	Weight         int               // 权重，默认为 100
	Calls          map[string]string // 调用的应用列表及其配置
	CallRetryTimes int               // 调用重试次数
	IpPrefix       string            // 指定使用的 IP 网段
}

// Config 存储发现服务的全局配置（兼容旧代码）
var Config = ConfigStruct{
	Weight:         100,
	CallRetryTimes: 10,
}

var configLock sync.RWMutex

// SetConfig 安全地设置全局配置
func SetConfig(conf ConfigStruct) {
	configLock.Lock()
	defer configLock.Unlock()
	Config = conf
}

// GetConfig 安全地获取全局配置
func GetConfig() ConfigStruct {
	configLock.RLock()
	defer configLock.RUnlock()
	return Config
}
