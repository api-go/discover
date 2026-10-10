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

支持 DISCOVER_REGISTRY/APP/WEIGHT/CALLS（CALLS 为 JSON map），configure 显式覆盖环境。缺省 H2C；`1` 为 HTTP/1.1。固定 GET /_discover/health 默认 200/OK；setHealthHandler 接收 (request)->(status,headers,bytes)。支持 WS/WSS，open 返回 websockets.sync 连接。

核心 API：init/start/stop/wait/isClient/isServer/newCaller/addExternalApp/getAppNodes/setNode/setRoute/setLoadBalancer/setLogger。Caller：get/post/put/delete/head/do/doWithNode/open；返回 httpx.Response，extensions['discoverNode'] 为实际节点。HTTP 响应缓冲，不提供 Go ManualDoWithNode 的流式对应接口。

POST 与 GET 对传输故障/502–504 同样换节点；应用保证幂等。标准 Header、X-App-* 自动续传。多个 worker 共用地址时由宿主/master 管理注册，不在单 worker 退出时注销共享节点。
