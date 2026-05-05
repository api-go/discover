# Discover

基于 Redis 的极简服务发现与负载均衡组件。

## 核心特性
- **自动注册与发现**: 基于 Redis 的服务节点自动注册、心跳维持及实时更新。
- **智能负载均衡**: 支持按权重分配、自动剔除故障节点、重试机制。
- **无感透传**: 自动处理微服务间的 Header 透传（如 TraceID、UserID 等）。
- **多协议支持**: 支持 HTTP/1.1、HTTP/2 (H2C)、WebSocket。

## 配置参考
```yaml
discover:
  registry: redis://127.0.0.1:6379/15 # 注册中心地址
  app: my-service                     # 当前应用名称
  weight: 100                         # 节点权重
  calls:                              # 调用的服务定义
    auth: 1s:my-token:2              # 服务名: 超时:Token:HTTP版本
    user: 500ms
```

## API 指南

### 初始化与启动
- `Start(addr string) bool`: 启动服务发现，指定当前节点的外部访问地址。
- `EasyStart() (string, int)`: 自动监听可用端口并启动服务发现。返回 IP 和端口。
- `Stop()`: 停止服务并注销节点。

### 服务调用 (Caller)
- `NewCaller(request *http.Request, logger *log.Logger) *Caller`: 创建调用器。传入原始请求可自动透传 Header。
- `Caller.Get / Post / Put / Delete / Head`: 发起同步请求。
- `Caller.Do(method, app, path, data, headers...)`: 发起通用请求，返回 `http.Result`。
- `Caller.Open(app, path, headers...)`: 发起 WebSocket 连接。

### 手动管理
- `AddExternalApp(app, callConf string)`: 手动添加需要发现的外部应用。
- `SetNode(app, addr string, weight int)`: 手动设置某个服务的节点信息。

### 负载均衡与路由
- `SetLoadBalancer(lb LoadBalancer)`: 自定义全局负载均衡策略。
- `SetRoute(route func(ac *AppClient, r *http.Request))`: 设置全局路由拦截规则。

## 环境变量
- `DISCOVER_REGISTRY`: 注册中心地址。
- `DISCOVER_APP`: 应用名。
- `DISCOVER_WEIGHT`: 节点权重。
- `DISCOVER_CALLS`: 调用的应用定义。
- `DISCOVER_LISTEN`: EasyStart 监听地址。
