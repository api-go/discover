# apigo-discover（v1.5.16 源码发布）

Ruby >=2.6，Redis 与原生 Net::HTTP。按项目约定公共方法使用 lowerCamelCase。

```ruby
app = ApigoDiscover.middleware(app) # Rack
ApigoDiscover.start(advertiseAddr)  # 由宿主统一启动
result = ApigoDiscover.newCaller.post('orders', '/submit', data)
ApigoDiscover.stop                 # 宿主关闭
```

核心 API：init/configure/start/stop/wait/isClient/isServer/newCaller/addExternalApp/getAppNodes/setNode/setRoute/setLoadBalancer/setLogger。Caller：get/post/put/delete/head/do/doWithNode/open；HTTP 响应为 statusCode/headers/body/node/native 哈希。Rack middleware 提供局部请求上下文，fromRack 可显式转换原生请求环境。

支持统一 DISCOVER_REGISTRY/APP/WEIGHT/CALLS（JSON map）；configure 显式覆盖环境。Net::HTTP 仅支持 HTTP/1.1，必须明确调用配置 `1`/`http`，遇到缺省 H2C 显式报错。HTTP/HTTPS 支持；WebSocket 当前仅 WS，WSS 显式报错，避免无主机名校验的 TLS 连接。

GET /_discover/health 未配置服务端注册或已确认注册时为 200/OK，常驻服务未确认注册时为 503；setHealthHandler 接收 Rack 风格 (request)->[status,headers,bodyEnumerable]。healthPath 返回固定路径。SDK 不巡检下游；POST/GET 同样重试传输故障和 502–504，应用负责幂等。X-App-* 按列表语义续传。

## Recovery lifecycle (unreleased, 2026-10-31)

Resident SDK Start starts recovery workers and returns without waiting for Redis (PHP-FPM only initializes its request-scoped client). Success means the lifecycle was started, **not** that registration is complete. Use `IsRegistered()` (Go) / `isRegistered()` (other resident SDKs) for the last confirmed registration/lease state. EasyStart returns the chosen address even while Redis is unavailable. Configure before Start; stop before changing registration configuration. Start during an existing run first drains that run with bounded network timeouts.

Recovery continues until Stop, without a retry count limit. Registration retries back off to about four seconds with jitter, and normal leases renew about once a second (PHP uses timer scheduling). Network timeouts and Redis load add to these intervals; there is no guaranteed global recovery deadline. A stopped process cannot recover itself.

Registration writes, lease renewal and recovery notifications use one Lua EVAL command. Snapshots atomically check leases and remove expired membership. Redis credentials must permit EVAL and the underlying hash, lease and Pub/Sub commands. The existing standalone Redis registry/key layout is retained; Redis Cluster cross-slot operation is not added.

Pub/Sub accelerates reconciliation; periodic snapshots (about two seconds) repair lost notifications, reconnect races and empty-registry restarts. Failed snapshots preserve last-known nodes. This improves availability during registry outages but does not guarantee that cached backends remain alive; local failure scoring and fallback probes still apply. Periodic snapshots are authoritative: publish a notification after writing membership and a lease, rather than publishing an unbacked node alone.

Stop cancels retries, joins in-flight workers, then performs best-effort unregister. No worker may register after Stop finishes. If unregister cannot reach Redis, its lease expires after ten seconds. Redis recovery networking uses finite connect/read/write or command timeouts. Custom application health handlers continue to override the default.

Default GET `/_discover/health` returns 503 while an active server has no confirmed registration and 200 after registration recovers. It reports the latest worker observation, not a synchronous Redis probe or a check of all business dependencies. Use a separate process-liveness endpoint if the supervisor must keep the process alive during a registry outage.

PHP-FPM does not own a persistent recovery worker or register a service. Its Start only initializes request-scoped discovery; calls use bounded Redis IO and may succeed on subsequent requests after Redis returns. Host registration remains the host/service manager's responsibility.

Local call failures never delete global registry membership. Failure scores affect only that caller; fallback probes allow cached backends to recover even while Redis is unavailable. Only owner shutdown and atomic expired-lease cleanup remove membership.
