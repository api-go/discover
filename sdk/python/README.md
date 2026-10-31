# apigo-discover（v1.5.16 源码发布）

Python >=3.10；本项目公共方法使用 lowerCamelCase。

```python
import apigo_discover as discover
application = discover.wsgi(application)  # ASGI: discover.asgi(application)
discover.start(advertiseAddr)             # 宿主绑定地址后调用一次
result = discover.newCaller().post('orders', '/submit', data)
discover.stop()                           # 宿主关闭
```

WSGI/ASGI middleware 自动携带请求上下文；Caller 同步调用，ASGI 业务使用 asyncio.to_thread。显式上下文包含 headers（多值）、clientIP/host/scheme/userAgent；fromASGI/fromWSGI 可转换原生输入，不读取入站 body。

支持 DISCOVER_REGISTRY/APP/WEIGHT/CALLS（CALLS 为 JSON map），configure 显式覆盖环境。缺省 H2C；`1` 为 HTTP/1.1。固定 GET /_discover/health 未配置服务端注册或已确认注册时为 200/OK，常驻服务未确认注册时为 503；setHealthHandler 接收 (request)->(status,headers,bytes)。支持 WS/WSS，open 返回 websockets.sync 连接。

核心 API：init/start/stop/wait/isClient/isServer/newCaller/addExternalApp/getAppNodes/setNode/setRoute/setLoadBalancer/setLogger。Caller：get/post/put/delete/head/do/doWithNode/open；返回 httpx.Response，extensions['discoverNode'] 为实际节点。HTTP 响应缓冲，不提供 Go ManualDoWithNode 的流式对应接口。

POST 与 GET 对传输故障/502–504 同样换节点；应用保证幂等。标准 Header、X-App-* 自动续传。多个 worker 共用地址时由宿主/master 管理注册，不在单 worker 退出时注销共享节点。

## Recovery lifecycle (unreleased, 2026-10-31)

Resident SDK Start starts recovery workers and returns without waiting for Redis (PHP-FPM only initializes its request-scoped client). Success means the lifecycle was started, **not** that registration is complete. Use `IsRegistered()` (Go) / `isRegistered()` (other resident SDKs) for the last confirmed registration/lease state. EasyStart returns the chosen address even while Redis is unavailable. Configure before Start; stop before changing registration configuration. Start during an existing run first drains that run with bounded network timeouts.

Recovery continues until Stop, without a retry count limit. Registration retries back off to about four seconds with jitter, and normal leases renew about once a second (PHP uses timer scheduling). Network timeouts and Redis load add to these intervals; there is no guaranteed global recovery deadline. A stopped process cannot recover itself.

Registration writes, lease renewal and recovery notifications use one Lua EVAL command. Snapshots atomically check leases and remove expired membership. Redis credentials must permit EVAL and the underlying hash, lease and Pub/Sub commands. The existing standalone Redis registry/key layout is retained; Redis Cluster cross-slot operation is not added.

Pub/Sub accelerates reconciliation; periodic snapshots (about two seconds) repair lost notifications, reconnect races and empty-registry restarts. Failed snapshots preserve last-known nodes. This improves availability during registry outages but does not guarantee that cached backends remain alive; local failure scoring and fallback probes still apply. Periodic snapshots are authoritative: publish a notification after writing membership and a lease, rather than publishing an unbacked node alone.

Stop cancels retries, joins in-flight workers, then performs best-effort unregister. No worker may register after Stop finishes. If unregister cannot reach Redis, its lease expires after ten seconds. Redis recovery networking uses finite connect/read/write or command timeouts. Custom application health handlers continue to override the default.

Default GET `/_discover/health` returns 503 while an active server has no confirmed registration and 200 after registration recovers. It reports the latest worker observation, not a synchronous Redis probe or a check of all business dependencies. Use a separate process-liveness endpoint if the supervisor must keep the process alive during a registry outage.

PHP-FPM does not own a persistent recovery worker or register a service. Its Start only initializes request-scoped discovery; calls use bounded Redis IO and may succeed on subsequent requests after Redis returns. Host registration remains the host/service manager's responsibility.

Local call failures never delete global registry membership. Failure scores affect only that caller; fallback probes allow cached backends to recover even while Redis is unavailable. Only owner shutdown and atomic expired-lease cleanup remove membership.
