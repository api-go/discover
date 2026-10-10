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

GET /_discover/health 默认 200/OK；setHealthHandler 接收 Rack 风格 (request)->[status,headers,bodyEnumerable]。healthPath 返回固定路径。SDK 不巡检下游；POST/GET 同样重试传输故障和 502–504，应用负责幂等。X-App-* 按列表语义续传。
