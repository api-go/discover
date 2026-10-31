# cc.apigo:discover（v1.5.16 源码发布）

Java 17，Jedis 注册中心及原生 java.net.http 客户端。

```java
server.createContext("/", Discover.middleware(handler));
server.start();
Discover.start(advertiseAddr);
// handler 中：
Discover.Result result = Discover.newCaller().post("orders", "/submit", payloadBytes);
// 宿主关闭：Discover.stop();
```

支持 DISCOVER_REGISTRY/APP/WEIGHT/CALLS（CALLS 为 JSON map），configure 显式覆盖环境。固定 GET /_discover/health 未配置服务端注册或已确认注册时为 200/OK，常驻服务未确认注册时为 503；setHealthHandler(HttpHandler) 自定义。缺省 HTTP/2/H2C（Upgrade）；`1` 为 HTTP/1.1。

核心方法 lowerCamelCase。Java 无法使用 do 关键字，也不能声明与 Object.wait 冲突的静态 wait；分别采用 doRequest、waitForStop。其他核心 API：init/start/stop/isClient/isServer/newCaller/addExternalApp/getAppNodes/setNode/setRoute/setLoadBalancer/setLogger。

Caller：get/post/put/delete/head/doRequest/doWithNode/open。HTTP 响应为 Result(statusCode,headers,body,node,nativeResponse)；open(app,path,WebSocket.Listener) 返回原生 WebSocket。HTTP body 使用可重放 byte[]，HTTP 响应缓冲。

HttpExchange 通过 RequestContext.from 转换；middleware 提供 ThreadLocal 自动上下文，异步跨线程须显式携带。尚未提供 Spring/Servlet 自动装配。POST 与 GET 同样换节点，应用负责幂等；固定 Header 与 X-App-* 自动续传。

## Recovery lifecycle (unreleased, 2026-10-31)

Resident SDK Start starts recovery workers and returns without waiting for Redis (PHP-FPM only initializes its request-scoped client). Success means the lifecycle was started, **not** that registration is complete. Use `IsRegistered()` (Go) / `isRegistered()` (other resident SDKs) for the last confirmed registration/lease state. EasyStart returns the chosen address even while Redis is unavailable. Configure before Start; stop before changing registration configuration. Start during an existing run first drains that run with bounded network timeouts.

Recovery continues until Stop, without a retry count limit. Registration retries back off to about four seconds with jitter, and normal leases renew about once a second (PHP uses timer scheduling). Network timeouts and Redis load add to these intervals; there is no guaranteed global recovery deadline. A stopped process cannot recover itself.

Registration writes, lease renewal and recovery notifications use one Lua EVAL command. Snapshots atomically check leases and remove expired membership. Redis credentials must permit EVAL and the underlying hash, lease and Pub/Sub commands. The existing standalone Redis registry/key layout is retained; Redis Cluster cross-slot operation is not added.

Pub/Sub accelerates reconciliation; periodic snapshots (about two seconds) repair lost notifications, reconnect races and empty-registry restarts. Failed snapshots preserve last-known nodes. This improves availability during registry outages but does not guarantee that cached backends remain alive; local failure scoring and fallback probes still apply. Periodic snapshots are authoritative: publish a notification after writing membership and a lease, rather than publishing an unbacked node alone.

Stop cancels retries, joins in-flight workers, then performs best-effort unregister. No worker may register after Stop finishes. If unregister cannot reach Redis, its lease expires after ten seconds. Redis recovery networking uses finite connect/read/write or command timeouts. Custom application health handlers continue to override the default.

Default GET `/_discover/health` returns 503 while an active server has no confirmed registration and 200 after registration recovers. It reports the latest worker observation, not a synchronous Redis probe or a check of all business dependencies. Use a separate process-liveness endpoint if the supervisor must keep the process alive during a registry outage.

PHP-FPM does not own a persistent recovery worker or register a service. Its Start only initializes request-scoped discovery; calls use bounded Redis IO and may succeed on subsequent requests after Redis returns. Host registration remains the host/service manager's responsibility.

Local call failures never delete global registry membership. Failure scores affect only that caller; fallback probes allow cached backends to recover even while Redis is unavailable. Only owner shutdown and atomic expired-lease cleanup remove membership.
