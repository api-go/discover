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

支持 DISCOVER_REGISTRY/APP/WEIGHT/CALLS（CALLS 为 JSON map），configure 显式覆盖环境。固定 GET /_discover/health 默认 200/OK；setHealthHandler(HttpHandler) 自定义。缺省 HTTP/2/H2C（Upgrade）；`1` 为 HTTP/1.1。

核心方法 lowerCamelCase。Java 无法使用 do 关键字，也不能声明与 Object.wait 冲突的静态 wait；分别采用 doRequest、waitForStop。其他核心 API：init/start/stop/isClient/isServer/newCaller/addExternalApp/getAppNodes/setNode/setRoute/setLoadBalancer/setLogger。

Caller：get/post/put/delete/head/doRequest/doWithNode/open。HTTP 响应为 Result(statusCode,headers,body,node,nativeResponse)；open(app,path,WebSocket.Listener) 返回原生 WebSocket。HTTP body 使用可重放 byte[]，HTTP 响应缓冲。

HttpExchange 通过 RequestContext.from 转换；middleware 提供 ThreadLocal 自动上下文，异步跨线程须显式携带。尚未提供 Spring/Servlet 自动装配。POST 与 GET 同样换节点，应用负责幂等；固定 Header 与 X-App-* 自动续传。
