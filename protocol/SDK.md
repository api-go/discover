# Discover SDK 接口与互通契约（开发中）

## 已确认边界

- 一进程一个应用，全局实例；请求上下文独立，禁止全局保存当前请求。
- 连续节点失败达到阈值仅在本地避让；不得删除全局注册或发布全局下线。全部节点均失败时允许本地试探，成功后清零。
- GET、POST 等使用相同的失败重试规则：传输故障和 HTTP 502/503/504；501 不重试。业务拒绝不是节点失效。
- 重试不保证业务只执行一次；状态校验与变更的原子性、幂等和重复提交应答均由应用处理。SDK/Gateway 不缓存业务应答。
- SDK/Gateway 不主动巡检其他节点；检测、重建和扩缩容属于服务管理调度器。
- 源码保持在 discover 下，各语言独立构建发布；默认共享全局实例，框架适配负责监听地址和生命周期。
- Go 复用现有 discover，无另一个 Go 模块；增加原生 HTTP 调用接口，保留已有 API。

## Redis 协议

沿用 v1.5.15：Hash key 为 app，field 为 host:port，value 为正整数权重；心跳 key 为 app_addr，值 1，TTL 10 秒，每秒续期。频道 CH_app，消息为 `addr weight`；weight=0 表示下线。重连先订阅，再全量读取并按心跳键过滤。正常注销 HDEL、DEL 心跳并发布下线。客户端失败只影响本地评分；只有持有生命周期的服务注销或原子快照发现租约过期才删除注册。

Pub/Sub 不是持久日志；约两秒的后台快照校准补偿丢失通知，读取失败保留最后成功快照。注册、租约与恢复通知通过一个 Lua EVAL 命令完成；异常持续重试（退避约至四秒并加入抖动），无次数上限。Redis 账号需允许 EVAL 及所调用命令。现有单机 Redis 布局不新增 Cluster 跨槽支持。生产 Redis 应隔离命名及访问权限。

## Header

固定续传：X-Real-IP、X-Forwarded-For、X-User-ID、X-Device-ID、X-Client-App-Name、X-Client-App-Version、X-Session-ID、X-Request-ID、X-Host、X-Scheme、X-User-Agent。所有 X-App-*（大小写不敏感）自动续传，保留所有值（允许重复字段或按 HTTP 列表语义逗号合并；应用扩展头应采用列表语义）。

X-From-App/X-From-Node 每次调用覆盖为当前调用方。Access-Token 由目标服务配置提供，不自动透传。外部入口负责可信 IP、身份字段的清理与认证；SDK 的自动续传不构成身份认证。入口缺失 Request ID 时生成；User-Agent/Host/Scheme 从原生请求补齐。没有入站请求的后台调用也生成 Request ID。

## API 名称

沿用 Go 的 Init、Start、Stop、Wait、EasyStart、IsClient、IsServer、IsRegistered、NewCaller、AddExternalApp、GetAppNodes、SetNode、SetLoadBalancer、SetRoute、SetLogger；Caller.Get/Post/Put/Delete/Head/Do/Open/DoWithNode/ManualDoWithNode。Java 因关键字/继承约束使用 doRequest、waitForStop；其他非 Go API 使用 lowerCamelCase（包括 Python、Ruby，按本项目约定）。其他语言 configure、Go 原生请求调用与健康处理器是新增能力，不能声称与旧 Go 完全一致。

核心请求上下文包含多值 Headers、ClientIP、Host、Scheme、UserAgent，以及语言可支持的取消/截止时间。不得读取入站请求体。框架适配从原生请求转换，业务可显式传上下文；后台调用允许空上下文。HTTP 响应保留原生应答及节点，HTTP 状态与传输错误分开。不可重放的流式 body 不自动重发。

## 健康接口

固定 GET /_discover/health：未配置服务端注册或最近一次注册/心跳已确认时返回 200、Content-Type text/plain; charset=utf-8、内容 OK；活动服务尚未注册或注册不可用时默认返回 503。其他方法返回 405。应用可替换处理器，自行返回状态码和内容；管理器以 200–299 表示健康，其他状态/传输错误表示不健康。接口不查 Redis、不扫描下游、不执行重试。核心只提供处理器，框架适配自动挂载；SDK 不自行开另一个 HTTP 端口。

## 配置与框架

统一 DISCOVER_REGISTRY、DISCOVER_APP、DISCOVER_WEIGHT、DISCOVER_CALLS；DISCOVER_LISTEN 当前为 Go EasyStart 配置，其他语言从宿主已绑定 listener 获取地址；新增配置应同步所有语言。DISCOVER_CALLS 使用 JSON 对象，callConf 缺省 HTTP 版本为 2（H2C）；不支持 H2C 的传输必须显式报错，调用方可设置 1/http。明确配置优先于环境变量；框架配置通过适配层桥接。健康处理器与请求上下文入口是核心 API。

首批语言 Go、Java、Python、Node.js/TypeScript、PHP、Ruby。C#/.NET 后续按实际需求增加。PHP 共用核心，FPM 独立入口：不创建跨请求心跳/订阅循环，不在请求结束注销服务，按需读取发现；常驻入口持有生命周期。多 worker 共用地址的注册由宿主/管理器持有，不能由单 worker 退出注销。

## 验收

连续失败及成功清零；HTTP 501 不重试、502–504 换节点，POST body 重放；多值 X-App-*；来源覆盖；入口元数据补齐；取消停止重试；健康默认及自定义状态/内容；Go 与其他语言 Redis 双向注册/调用/注销、断线恢复；框架和 PHP FPM 生命周期。各项必须记录实际验证结果，未实现/未验证不得标记通过。

## 本地未发布：启动与恢复生命周期

常驻 Start/start 只启动后台恢复任务，不等待 Redis；成功返回不代表已注册。Go IsRegistered、其他语言 isRegistered 提供最近确认状态。主进程可继续监听 HTTP，就绪检查单独判断。Start 遇到非法本地配置可及时报错；重启已有生命周期时先用有界网络超时排空旧任务。Stop 停止并等待所有恢复任务，然后尽力注销；注销不可达时依赖租约过期。后台任务不能在 Stop 返回后重新注册。PHP-FPM 不拥有常驻恢复任务，start 不探测 Redis，后续请求按需读取发现。
