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

### 全局配置
- `Config.Registry / App / Weight`: 注册中心、当前应用名与节点权重。
- `Config.Calls`: 被调用应用及调用配置。
- `Config.CallRetryTimes`: 节点连续失败摘除阈值；成功后清零。每次调用不重复尝试同一节点。
- `Config.IpPrefix`: `EasyStart` 选择监听地址时使用的 IP 前缀。
- `RelayHeaders`: 跨服务调用时自动透传的 Header 列表。

### 初始化与启动
- `Start(addr string) bool`: 启动服务发现，指定当前节点的外部访问地址。
- `EasyStart() (string, int)`: 自动监听可用端口并启动服务发现。返回 IP 和端口。
- `Stop()`: 停止服务，等待正在进行的心跳退出后注销节点（v1.5.15）。

### 服务调用 (Caller)
- `NewCaller(request *http.Request, logger *log.Logger) *Caller`: 创建调用器。传入原始请求可自动透传 Header。
- `Caller.Get / Post / Put / Delete / Head`: 发起同步请求。
- `Caller.Do(method, app, path, data, headers...)`: 发起通用请求，返回 `http.Result`。
- `Caller.Open(app, path, headers ...http.Header) (*gohttp.WebSocketConn, *http.Response, string, error)`：返回连接、握手响应、节点和错误。
- `Caller.OpenContext(ctx, app, path, headers ...http.Header)`：显式指定连接生命周期 Context。

### 手动管理
- `AddExternalApp(app, callConf string)`: 手动添加需要发现的外部应用。
- `GetAppNodes(app string)`: 获取应用当前可用节点。
- `SetNode(app, addr string, weight int)`: 手动设置某个服务的节点信息。
- `Init / Start / Stop / Wait`: 管理全局单实例生命周期；Start/Stop 串行，重复 Start 替换心跳循环，Wait 可重复调用或有多个等待者。Config/SetLogger 应在启动前设置，不提供任意并发配置写入的保证。
- `IsClient / IsServer`: 判断当前运行角色。
- `SetLogger(logger)`: 设置模块日志器。

### 负载均衡与路由
- `SetLoadBalancer(lb LoadBalancer)`: 自定义全局负载均衡策略。
- `LoadBalancer.Next`: 从可用节点中选择本次调用节点。
- `LoadBalancer.Response`: 请求完成后更新节点状态或统计信息。
- `DefaultLoadBalancer`: 内置的权重得分负载均衡实现。
- `SetRoute(route func(ac *AppClient, r *http.Request))`: 设置全局路由拦截规则。
- `Caller.DoWithNode / ManualDoWithNode`: 指定或返回实际调用节点。

## 环境变量
- `DISCOVER_REGISTRY`: 注册中心地址。
- `DISCOVER_APP`: 应用名。
- `DISCOVER_WEIGHT`: 节点权重。
- `DISCOVER_CALLS`: 调用的应用定义。
- `DISCOVER_LISTEN`: EasyStart 监听地址。

## v1.5.16：多语言 SDK 与原生 HTTP 接入

SDK 位于 `sdk/`，包括 Python、Node.js/TypeScript、Java、PHP 常驻、独立 PHP FPM 入口、Ruby；Go 直接使用本模块，无需 go/service。[接口对照与接入](sdk/README.md)、[共同协议](protocol/SDK.md)、[验收后版本联动](RELEASE-CHECKLIST.md)。

- Header 统一为 X-Real-IP、X-From-App、X-From-Node、X-User-Agent；自动续传 X-App-*（大小写不敏感）。来源应用/节点按当前调用方覆盖。旧 X-Discover-From-* 不再输出；Service 随依赖升级统一读取这些字段。
- `HealthPath`：GET /_discover/health，默认 200/OK。`HealthHandler()` 提供原生处理器，`SetHealthHandler(http.Handler)` 自定义应答，nil 恢复默认；`Middleware(next)` 挂载健康路径并补齐缺失的请求元数据；`PrepareRequest(r)` 供框架在解析可信 IP 后单独补齐。入口须自行处理代理信任和身份认证。
- `Caller.DoRequest(app, *http.Request)` 返回原生 *http.Response、节点地址、error；URL 使用本地绝对路径，调用方负责关闭响应 Body。请求 body 有 GetBody 时可换节点重放，无 GetBody 的流只尝试一次。SetRoute 可修改原生调用元数据（App/Method/Path/Headers），原生 body 不通过 AppClient.Data 修改。
- `SetHTTPClient(*http.Client)` 注入原生客户端，nil 恢复默认；SDK 不关闭外部提供的客户端。
- `NodeInfo.State()` 返回并发安全的 weight/used/failed 快照；自定义负载均衡器使用 State，避免并发读写公开计数字段。
- GET/POST 均对传输故障和 502–504 换节点；501 不重试。成功清零节点失败计数，达到连续失败阈值全局摘除，存活服务心跳可重注册。取消请求不代表节点失效；SDK 不保证业务只执行一次，幂等与原子状态迁移由应用负责。
- 不新增 SDK/Gateway 主动巡检，调度/管理器负责检测与重建。

本次发布已对齐 Log v1.5.17、HTTP v1.5.11 和 Redis v1.5.20；SDK 源码随 v1.5.16 tag 提供，未上传各语言包仓库。Service/Gateway 按已发布 tag 继续升级，均不新增下游主动检测。

## v1.5.16 依赖基线

本版本对齐 Log v1.5.17 与发布时模块索引中的上游已发布依赖；公开 API 保持兼容。

## 本地未发布 Open 接口

Go 客户端与 go/http 统一使用 Open / OpenContext；Discover 额外返回节点地址，旧 Open 签名删除。调用方负责关闭连接或失败响应 Body。

```go
conn, handshake, node, err := caller.Open("chat", "/ws", http.Header{"Cookie": {"session=one"}})
```

多值头、Cookie、Origin、子协议、SetRoute 与服务令牌保持传递。401/403、路径/协议拒绝不重试、不累计节点失败；网络故障和 502/503/504 在握手阶段切换节点，最后一次失败响应仍返回。建立后不自动重连。

配置超时只约束握手，父 Context 控制已建立连接。`Caller.WebSocketClient *gohttp.Client` 可注入私有 CA、mTLS、自定义拨号和独立 WebSocket Dialer。每次尝试复制客户端配置，不修改共享客户端。拒绝正文最多保留 1024 字节，长度由 go/http 归一化。

Redis 订阅与停止调用移至应用锁之外，并将动态注册与 Start/Stop 串行，避免重连回调造成锁反转。发布验证必须使用正式 HTTP tag，不能依赖本地工作区替换。

## 外部地址接入（v1.5.19）

原生入口可调用 `PrepareExternalRequest(request, resolver)`，resolver 为 HTTP 模块显式可信对端策略；错误须在继续处理前拒绝。Service 自动完成此流程。`PrepareRequest` 和 Caller 的请求级多值快照优先使用 HTTP 已固定的安全解析结果，即使 Header 后续改变也不重新解释外部地址。

未显式接入的原生请求仍保留旧 X-Host/X-Scheme 原始上下文兼容行为，它不是可信解析结果、认证凭据或内部 RPC 目标。默认不增加 Origin 续传；RPC 的 URL、Host、服务令牌及 TLS 校验目标不因外部地址变化。透明代理通过显式请求头保留 Origin。

## 默认内部续传（开发中，未发布）

受控服务链无需配置可信来源：外层 Gateway 生成规范 X-Host/X-Scheme，Service 在生成缺失 Request-ID 前解析，内部携带 Request-ID 的请求沿用最初外部地址。原生 HTTP 入口可用 `PrepareExternalRequest(r, nil)` 采用同一默认策略；也可显式传入可选 direct/trusted 策略。RPC 目标、认证和 TLS 语义不变，不默认续传 Origin。正式依赖 HTTP v1.5.16，无需本地 replace。
