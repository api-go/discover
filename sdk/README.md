# Discover 多语言 SDK（v1.5.16 源码发布）

协议见 [SDK 契约](../protocol/SDK.md)，发布顺序和用户的提交门槛见 [发布提醒](../RELEASE-CHECKLIST.md)。本次随 Discover v1.5.16 tag 发布源码，尚未上传 npm/PyPI/Maven/Composer/Gem 包仓库。请从该 tag 的 sdk/ 子目录安装或构建；包版本统一为 1.5.16。

## 接口与接入

所有 SDK 默认全局实例，使用 Init/Start/Stop 和 NewCaller 这一套概念。非 Go 方法统一 lowerCamelCase，包括 Python、Ruby。不是逐字相同的签名：Go 保留旧 API；其他语言使用各自响应类型、错误/异常和参数类型。Java 因 `do` 关键字和 Object.wait 限制使用 doRequest、waitForStop。

| 能力 | Go | 其他语言 |
|---|---|---|
| 初始化、角色、启停 | Init/Start/Stop/Wait/IsClient/IsServer | init/start/stop/wait/isClient/isServer（Java waitForStop） |
| 配置 | Config + Init | config/configure + init |
| 节点管理 | AddExternalApp/GetAppNodes/SetNode | addExternalApp/getAppNodes/setNode |
| 请求级调用器 | NewCaller(*http.Request, logger) | newCaller(原生请求或归一化上下文)，middleware 内可缺省请求 |
| HTTP | Get/Post/Put/Delete/Head/Do/DoWithNode | get/post/put/delete/head/do/doWithNode（Java doRequest） |
| 扩展 | SetRoute/SetLoadBalancer/SetLogger | setRoute/setLoadBalancer/setLogger |
| 健康 | HealthPath/HealthHandler/SetHealthHandler/Middleware | healthPath/healthHandler/setHealthHandler/middleware（Ruby healthPath()） |
| 原生流式响应 | Caller.DoRequest，调用方 Close body | 第一版其他 SDK 返回缓冲响应；未提供 ManualDoWithNode 的流式对应接口 |
| 自动监听地址 | 旧 EasyStart 保留 | 由宿主已绑定的 listener 提供地址，避免抢占端口/重复启动服务器 |

客户端提供按应用名调用，不生成业务接口、DTO 或业务错误处理。POST 和 GET 重试相同的传输错误及 502–504，501/业务拒绝不重试。应用负责原子状态变更和幂等。未承诺 exactly-once。

缺省 callConf 使用 HTTP 版本 2，和 Go 一致。可用 `1`、`http`、`2`、`h2c`、`https`，超时建议显式 `500ms`/`10s`，Token 使用已有 `超时:Token:版本` 形式；含冒号的 Token 请通过支持结构化配置的语言接口设置或单独 Header。DISCOVER_CALLS 为 JSON 对象。configure 显式配置在 init 读取环境后覆盖；全局 Config 在启动前设置，不支持运行时任意并发修改。

## 传输边界

| SDK | HTTP/1.1 | H2C | WebSocket | 请求上下文适配 |
|---|---|---|---|---|
| Go | 支持 | 支持 | WS/WSS | 原生 net/http |
| Python | 支持 | 支持（HTTPTransport 强制 HTTP/2） | WS/WSS | ASGI、WSGI |
| Node.js/TypeScript | 支持 | 支持 | WS/WSS | IncomingMessage、请求级 AsyncLocalStorage |
| Java 17 | 支持 | 支持（HTTP/2 Upgrade） | WS/WSS，应用提供 Listener | HttpExchange、请求级 ThreadLocal |
| PHP 常驻 | 支持 | 支持 | WS/WSS | Swoole request，协程上下文 |
| PHP FPM | 支持 | curl 编译支持 HTTP/2 时支持 | 不持有会话 | PSR-7 或 $_SERVER |
| Ruby | 支持 | 默认 Net::HTTP 不支持，显式报错 | WS；WSS 显式报错 | Rack、线程/协程局部上下文 |

Ruby 不把 H2C/WSS 静默降级，也不关闭 TLS 校验。其他 SDK 的 TLS/WSS 生产证书路径尚未纳入本轮互通测试。Node/Python 主要异步框架、Java Spring/Servlet、PHP Laravel、Ruby Rails 的完整自动生命周期装配尚未实现；本轮提供核心与通用 middleware，不能把这些框架标为已验收。

Python Caller 是同步客户端；ASGI 业务应通过 `asyncio.to_thread(caller.post, ...)` 调用，避免阻塞事件循环。WSGI 服务器/多进程框架由 master/宿主拥有注册生命周期，单 worker 不注销共享服务地址。

PHP 常驻入口需要 Swoole 和 Redis 扩展，使用协程 HTTP 客户端，不依赖 curl coroutine Hook。FPM 独立包 `apigo/discover-fpm` 共用核心，按请求读取发现；不会注册、心跳、订阅或注销服务。FPM 的节点注册与管理由宿主/服务管理器持有。

固定健康接口 GET /_discover/health，未配置服务端注册或已确认注册时为 200/OK，常驻服务未确认注册时为 503，其他方法 405；应用可覆盖状态码及内容。SDK 不扫描其他节点。外部入口负责信任 IP/身份来源，自动续传不构成身份认证。应用扩展头使用 X-App-*，匹配忽略大小写，所有值均保留；HTTP 客户端可能按列表语义将重复字段合并为逗号分隔值。

## 最小接入

Go 原生框架不需要 go/service：

```go
// listener 已经绑定；Config 在启动前设置。
discover.Start(listener.Addr().String())
defer discover.Stop()
server := &http.Server{Handler: discover.Middleware(appHandler)}
// 在 handler 中：
caller := discover.NewCaller(r, nil)
request, _ := http.NewRequestWithContext(r.Context(), "POST", "/orders", strings.NewReader(payload))
response, node, err := caller.DoRequest("orders", request)
// response 非 nil 时由应用 Close response.Body。
```

Node.js：

```js
import * as discover from '@apigo/discover';
const server = http.createServer(discover.middleware(handler));
// server 实际 listen 完成后，仅一次：
await discover.start(advertiseAddr);
// handler 中无需再传入 request：
const result = await discover.newCaller().post('orders', '/submit', data);
// 宿主关闭时 await discover.stop()。
```

Python WSGI / ASGI：

```python
import apigo_discover as discover
application = discover.wsgi(application)  # ASGI 使用 discover.asgi(application)
# 宿主绑定完监听地址后：
discover.start(advertiseAddr)
# middleware 请求范围内：
result = discover.newCaller().post('orders', '/submit', data)
```

Java：`server.createContext("/", Discover.middleware(handler))`；监听成功后 `Discover.start(advertiseAddr)`；handler 内 `Discover.newCaller().post(app, path, bytes)`。异步移交到其他线程时显式携带 RequestContext。

PHP Swoole：`$server->handle('/', Discover::swooleMiddleware($handler))`；由宿主注册 Start/Stop。FPM 使用 `Apigo\Discover\Fpm\Discover`；`newCaller()` 自动从当前请求的 $_SERVER 获取上下文，也可传 PSR-7 请求。

Ruby Rack：`app = ApigoDiscover.middleware(app)`；宿主启动/停止时调用 `start(advertiseAddr)`/`stop`；handler 内 `ApigoDiscover.newCaller.post(app, path, data)`。

## 本地验证

Go 使用 GOWORK=off，确保仍使用 go.mod 的已发布依赖。Redis 为现有本机 127.0.0.1:6379/15，测试不启动/停止用户的 Redis。

```sh
GOWORK=off go test -v ./...
GOWORK=off go test -bench=. ./...
DISCOVER_REDIS_INTEGRATION=1 GOWORK=off go test -race ./...
npm --prefix sdk/nodejs ci
npm --prefix sdk/nodejs test
python -m pip install -e sdk/python
python -m unittest discover -s sdk/python -p test_sdk.py -v
mvn -f sdk/java/pom.xml test
# Ruby：安装 gemspec 的依赖；PHP：启用 redis/curl，常驻另需 Swoole。
DISCOVER_PYTHON=你的Python环境路径 python sdk/conformance/run.py
```

跨语言 runner 使用 discover-conformance-* 键，验证客户端注册/注销、POST、上下文、H2C、WebSocket及 Go 调用其他语言的健康接口，并清理其注册数据/测试进程。PHP FPM 核心按请求模式独立测试；没有启动真正的 FPM master。

## Recovery lifecycle (unreleased, 2026-10-31)

Resident SDK Start starts recovery workers and returns without waiting for Redis (PHP-FPM only initializes its request-scoped client). Success means the lifecycle was started, **not** that registration is complete. Use `IsRegistered()` (Go) / `isRegistered()` (other resident SDKs) for the last confirmed registration/lease state. EasyStart returns the chosen address even while Redis is unavailable. Configure before Start; stop before changing registration configuration. Start during an existing run first drains that run with bounded network timeouts.

Recovery continues until Stop, without a retry count limit. Registration retries back off to about four seconds with jitter, and normal leases renew about once a second (PHP uses timer scheduling). Network timeouts and Redis load add to these intervals; there is no guaranteed global recovery deadline. A stopped process cannot recover itself.

Registration writes, lease renewal and recovery notifications use one Lua EVAL command. Snapshots atomically check leases and remove expired membership. Redis credentials must permit EVAL and the underlying hash, lease and Pub/Sub commands. The existing standalone Redis registry/key layout is retained; Redis Cluster cross-slot operation is not added.

Pub/Sub accelerates reconciliation; periodic snapshots (about two seconds) repair lost notifications, reconnect races and empty-registry restarts. Failed snapshots preserve last-known nodes. This improves availability during registry outages but does not guarantee that cached backends remain alive; local failure scoring and fallback probes still apply. Periodic snapshots are authoritative: publish a notification after writing membership and a lease, rather than publishing an unbacked node alone.

Stop cancels retries, joins in-flight workers, then performs best-effort unregister. No worker may register after Stop finishes. If unregister cannot reach Redis, its lease expires after ten seconds. Redis recovery networking uses finite connect/read/write or command timeouts. Custom application health handlers continue to override the default.

Default GET `/_discover/health` returns 503 while an active server has no confirmed registration and 200 after registration recovers. It reports the latest worker observation, not a synchronous Redis probe or a check of all business dependencies. Use a separate process-liveness endpoint if the supervisor must keep the process alive during a registry outage.

PHP-FPM does not own a persistent recovery worker or register a service. Its Start only initializes request-scoped discovery; calls use bounded Redis IO and may succeed on subsequent requests after Redis returns. Host registration remains the host/service manager's responsibility.

Local call failures never delete global registry membership. Failure scores affect only that caller; fallback probes allow cached backends to recover even while Redis is unavailable. Only owner shutdown and atomic expired-lease cleanup remove membership.
