package discover

import (
	"time"
	"apigo.cc/go/safe"
)

// CallConfig 下游服务调用配置
type CallConfig struct {
	Timeout time.Duration    // 请求超时时间
	Token   *safe.SafeBuf    // 访问凭证 (必须安全存储)
	Http2   bool             // 是否强制使用 HTTP/2 (H2C/H2)
	SSL     bool             // 是否使用 HTTPS/WSS
}

// Config 存储发现服务的可选配置
type Config struct {
	Weight         int                    // 权重，默认为 100
	Calls          map[string]CallConfig  // 调用的应用列表及其配置
	CallRetryTimes int                    // 调用重试次数
}
