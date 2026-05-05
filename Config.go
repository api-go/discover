package discover

// Config 存储发现服务的全局配置
var Config = struct {
	Registry       string            // 注册中心地址，如 redis://:@127.0.0.1:6379/15
	App            string            // 当前应用名称
	Weight         int               // 权重，默认为 100
	Calls          map[string]string // 调用的应用列表及其配置
	CallRetryTimes int               // 调用重试次数
	IpPrefix       string            // 指定使用的 IP 网段
}{
	Weight:         100,
	CallRetryTimes: 10,
}
