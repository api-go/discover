# apigo/discover 常驻 PHP SDK（v1.5.16 源码发布）

PHP >=7.4、Redis 扩展、curl 扩展；常驻模式需要 Swoole。使用 Swoole 协程 HTTP/HTTP2 客户端，避免依赖 curl 协程 Hook。

```php
use Apigo\Discover\Discover;
$server->handle('/', Discover::swooleMiddleware($handler));
Discover::start($advertiseAddr); // 宿主拥有的真实监听地址
$result = Discover::newCaller()->post('orders', '/submit', $data);
Discover::stop();               // 由宿主生命周期调用
```

核心 API lowerCamelCase：init/configure/start/stop/wait/isClient/isServer/newCaller/addExternalApp/getAppNodes/setNode/setRoute/setLoadBalancer/setLogger。Caller：get/post/put/delete/head/do/doWithNode/open。HTTP 响应为 statusCode/headers/body/node 数组，body 缓冲；不接收不可重放的资源流。open 返回 Swoole HTTP WebSocket 客户端，应用通过 push/recv/close 使用。

newCaller 可接收 PSR-7 request 或归一化数组；Swoole middleware 提供当前协程请求上下文。支持 DISCOVER_REGISTRY/APP/WEIGHT/CALLS（JSON map）；configure 显式覆盖环境。

固定 GET /_discover/health 默认 200/OK；setHealthHandler((request)=>[status,headers,body]) 自定义。POST 与 GET 同样对传输故障/502–504 换节点；应用负责幂等。标准 Header 和 X-App-* 自动续传。多 worker 的共享服务地址必须由宿主统一持有注册生命周期。

传统 FPM 使用独立入口包 apigo/discover-fpm，不在每个请求中调用常驻 start/stop。
