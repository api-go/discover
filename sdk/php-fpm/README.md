# apigo/discover-fpm（v1.5.16 源码发布）

共用 apigo/discover 的调用与协议核心；FPM 入口不注册服务、不启动心跳/订阅，不在请求结束注销共享服务。

```php
use Apigo\Discover\Fpm\Discover;
$result = Discover::newCaller()->post('orders', '/submit', $data);
```

newCaller 自动初始化按请求的客户端，从 $_SERVER 取得上下文；也可显式传 PSR-7 请求。支持统一环境变量及 configure。发现按需读取 Redis，使用原生 curl；H2C 要求 curl 编译支持 HTTP/2。不会持有 WebSocket 会话。

健康接口的数组处理器与常驻模式相同，需宿主/router 挂载固定 GET /_discover/health；默认 200/OK。服务注册/健康检测/重建由宿主或服务管理器承担。

本地 Composer 使用 ../php 的 path 仓库；正式发布后使用普通包依赖。验证使用请求模式 CLI 入口及内置 HTTP host，尚未启动真实 FPM master。

## Recovery lifecycle (unreleased, 2026-10-31)

Resident SDK Start starts recovery workers and returns without waiting for Redis (PHP-FPM only initializes its request-scoped client). Success means the lifecycle was started, **not** that registration is complete. Use `IsRegistered()` (Go) / `isRegistered()` (other resident SDKs) for the last confirmed registration/lease state. EasyStart returns the chosen address even while Redis is unavailable. Configure before Start; stop before changing registration configuration. Start during an existing run first drains that run with bounded network timeouts.

Recovery continues until Stop, without a retry count limit. Registration retries back off to about four seconds with jitter, and normal leases renew about once a second (PHP uses timer scheduling). Network timeouts and Redis load add to these intervals; there is no guaranteed global recovery deadline. A stopped process cannot recover itself.

Registration writes, lease renewal and recovery notifications use one Lua EVAL command. Snapshots atomically check leases and remove expired membership. Redis credentials must permit EVAL and the underlying hash, lease and Pub/Sub commands. The existing standalone Redis registry/key layout is retained; Redis Cluster cross-slot operation is not added.

Pub/Sub accelerates reconciliation; periodic snapshots (about two seconds) repair lost notifications, reconnect races and empty-registry restarts. Failed snapshots preserve last-known nodes. This improves availability during registry outages but does not guarantee that cached backends remain alive; local failure scoring and fallback probes still apply. Periodic snapshots are authoritative: publish a notification after writing membership and a lease, rather than publishing an unbacked node alone.

Stop cancels retries, joins in-flight workers, then performs best-effort unregister. No worker may register after Stop finishes. If unregister cannot reach Redis, its lease expires after ten seconds. Redis recovery networking uses finite connect/read/write or command timeouts. Custom application health handlers continue to override the default.

Default GET `/_discover/health` returns 503 while an active server has no confirmed registration and 200 after registration recovers. It reports the latest worker observation, not a synchronous Redis probe or a check of all business dependencies. Use a separate process-liveness endpoint if the supervisor must keep the process alive during a registry outage.

PHP-FPM does not own a persistent recovery worker or register a service. Its Start only initializes request-scoped discovery; calls use bounded Redis IO and may succeed on subsequent requests after Redis returns. Host registration remains the host/service manager's responsibility.

Local call failures never delete global registry membership. Failure scores affect only that caller; fallback probes allow cached backends to recover even while Redis is unavailable. Only owner shutdown and atomic expired-lease cleanup remove membership.
