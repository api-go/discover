# apigo/discover-fpm（v1.5.16 源码发布）

共用 apigo/discover 的调用与协议核心；FPM 入口不注册服务、不启动心跳/订阅，不在请求结束注销共享服务。

```php
use Apigo\Discover\Fpm\Discover;
$result = Discover::newCaller()->post('orders', '/submit', $data);
```

newCaller 自动初始化按请求的客户端，从 $_SERVER 取得上下文；也可显式传 PSR-7 请求。支持统一环境变量及 configure。发现按需读取 Redis，使用原生 curl；H2C 要求 curl 编译支持 HTTP/2。不会持有 WebSocket 会话。

健康接口的数组处理器与常驻模式相同，需宿主/router 挂载固定 GET /_discover/health；默认 200/OK。服务注册/健康检测/重建由宿主或服务管理器承担。

本地 Composer 使用 ../php 的 path 仓库；正式发布后使用普通包依赖。验证使用请求模式 CLI 入口及内置 HTTP host，尚未启动真实 FPM master。
