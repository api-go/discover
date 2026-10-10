# @apigo/discover（v1.5.16 源码发布）

Node.js >=20，JavaScript/TypeScript 共用；全局实例、请求级调用器。

```js
import * as discover from '@apigo/discover';
const server = http.createServer(discover.middleware(handler));
// listen 完成后：
await discover.start(advertiseAddr);
// handler 内自动取得当前入站上下文：
const result = await discover.newCaller().post('orders', '/submit', data);
// 宿主停止：
await discover.stop();
```

支持 DISCOVER_REGISTRY/APP/WEIGHT/CALLS（CALLS 为 JSON map），configure 显式覆盖环境。缺省 H2C；调用配置 `1` 使用 HTTP/1.1。支持 WS/WSS；open 返回 ws 对象，连接建立后不自动重放消息。

固定 GET /_discover/health 默认 200/OK；setHealthHandler((req,res)=>...) 自定义。SDK 不检测下游。标准 Header 与 X-App-* 自动续传，来源字段覆盖为当前服务；POST/GET 对传输故障及 502–504 同样换节点，应用负责幂等。

核心 API：init/start/stop/wait/isClient/isServer/newCaller/addExternalApp/getAppNodes/setNode/setRoute/setLoadBalancer/setLogger。Caller：get/post/put/delete/head/do/doWithNode/open；响应为 {statusCode, headers, body:Buffer, node, native}。body 使用可重放字符串、Buffer 或 JSON 对象，不接收流。

自动请求上下文要求 middleware 范围内；后台任务可显式传上下文。配置及扩展策略在启动前设置。HTTP 响应缓冲，不提供 Go ManualDoWithNode 的流式对应接口。
