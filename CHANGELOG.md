
## v1.5.24（2026-11-14）

依赖联动：safe v1.5.6 / crypto v1.5.11；当前使用显式本地 replace 验证，发布前移除并使用正式 tag。本轮同时将所引用的其他待发布下游模块提升到本轮正式版本；显式 replace 覆盖完整依赖链。本轮只更新依赖，不改变本模块 API。

## [v1.5.23] - 2026-11-12

- 发布已验收改动，使用 GitHub 正式依赖并移除临时本地 replace。

- 未传 logger 时也为诊断日志绑定入口 RequestID，保证失败记录与调用摘要使用同一 TraceID。
- 显式联动实时压缩日志及文件级字典依赖，兼容明文 opt-out；正式依赖已发布并移除开发 replace。

- 新增保留调用链 TraceID 的精简 CallLog；HTTP、原生请求和 WebSocket 握手分别记录完成阶段。
- Caller.SuppressCallLog 允许由上层等价 ProxyLog 委托记录摘要，不影响下游 RequestLog 或异常诊断。

## [v1.5.22] - 2026-11-10

- 迁移至 github.com/api-go/discover，同步内部模块路径与依赖。

## [Unreleased] - 2026-10-31
- Native Go and Python/Node.js/Java/Ruby/resident PHP now start recovery asynchronously, without requiring Redis at startup; IsRegistered/isRegistered reports registration separately.
- Continuous bounded backoff, atomic membership/lease renewal and recovery notifications, Pub/Sub reconnect plus periodic authoritative snapshots; failed snapshots retain last-known nodes.
- Stop cancels and joins recovery before unregister; default server health reports 503 until registration is confirmed. EasyStart no longer discards its address because Redis is down.
- Redis development dependency uses explicit ../redis replace for StartAsync and cancellable subscription lifecycle. No release or deployment performed; Service integration is validated separately.
- Call failures no longer delete global registry membership or synchronously contact Redis; local failure scoring and fallback probes isolate network partitions.
- PHP-FPM initializes without Redis IO; it remains request-scoped and never registers a service.

## [v1.5.19] - 2026-10-23

## v1.5.20（2026-10-26）
- 升级正式依赖以统一选用 crypto v1.5.9；AES-GCM 公共解密自动兼容内嵌随机 IV 和原固定 IV 密文。
- 现有公开 API 不变；接收端升级后再启用自动 IV 写入，原 CBC/SM4 算法格式不变。
- 验证默认内部 Request-ID 地址续传，不要求额外配置，RPC 目标、令牌及默认 Origin 续传语义保持不变。

## [v1.5.18] - 2026-10-23
- Explicit native ingress external address preparation and pinned normalized relay context; preserve RPC target and authentication semantics.

# CHANGELOG

## [v1.5.17] - 2026-10-14
- Redis 订阅/停止在应用锁之外执行，动态注册与 Start/Stop 串行，避免重连 reset 回调造成锁反转。
- Open / OpenContext 直接返回连接、握手响应、节点及错误，使用多值 http.Header，不保留旧签名。
- 业务握手拒绝不重试、不摘节点；网络故障和 502/503/504 保留 failover，并返回最后一次响应。
- 复用 go/http Open，支持 Caller.WebSocketClient 注入 TLS / Dialer；握手超时与连接生命周期分离。

## [v1.5.16] - 2026-10-10
- 发布多语言 SDK 核心及独立 PHP FPM 入口；统一 Header 与应用头续传；提供原生 HTTP/健康接口；连续失败摘除并在成功后清零；保留应用层幂等责任。对齐 Log v1.5.17 及当前已发布的上游依赖。

## [v1.5.15] - 2026-10-06
- 心跳使用独立停止/完成 channel，捕获注册元数据，消除退出标志及重启时元数据竞争。
- Start/Stop 串行，Stop 等待心跳退出后注销；Wait 支持多个等待者及重复调用。
- 对齐 config v1.5.8、http v1.5.10、redis v1.5.18。
- 增加重复启动、停止、重启与多等待者 Redis 集成验证。

## [v1.5.14] - 2026-09-12
- **测试隔离**: Redis 服务发现集成测试改为通过 `DISCOVER_REDIS_INTEGRATION=1` 显式启用。

## [v1.5.12] - 2026-09-05
- **依赖对齐**: 补齐 `config`、`file`、`http`、`log` 与 `redis` 的级联版本。

## [v1.5.11] - 2026-09-05
- **依赖对齐**: 升级全部 `github.com/api-go/*` 依赖至当前已发布版本。

## [v1.5.9] - 2026-08-25
- **仓库清理**: 忽略本地日志元数据文件，避免运行时状态进入版本控制。
- **依赖升级**: 对齐 `log`、`redis` 与 `shell` 的当前稳定补丁版本。

## [v1.5.8] - 2026-07-19
- **Redis API 对齐**: 服务注册、心跳、注销和节点拉取统一改用 `redis v1.5.11` 类型化命令，减少通用结果转换。
- **依赖升级**: 对齐 `cast`、`config`、`crypto`、`encoding`、`file`、`http`、`id`、`log`、`rand`、`safe`、`shell` 的当前 patch 版本。
- **文档与测试**: 补齐全局配置和负载均衡 API 说明，新增默认负载均衡器 Benchmark。

## [v1.5.7] - 2026-07-18
- **依赖校验修复**: 升级 `redis` 至 `v1.5.11`，并将间接依赖 `redigo` 固定为稳定版 `v1.9.3`，移除撤回版本。

## [v1.5.6] - 2026-07-18
- **依赖升级**: 升级全部 `github.com/api-go/*` 依赖到当前版本，移除已废弃的 `convert` 间接依赖。
- **Checksum 修复**: 淘汰内容已变化的 `crypto/safe v1.0.4` 依赖记录，重新生成可信的 `go.sum`。

## v1.0.0
- 从 `ssgo/discover` 迁移至 `github.com/api-go/discover`。
- 采用全新的 `apigo.cc/go` 基础设施（log, redis, http, cast, u）。
- 优化了注册中心同步机制，使用 `redis.Subscribe` 简化 PubSub 处理。
- 增强了负载均衡算法，引入更精确的得分计算。
- 统一了 Header 定义，对齐 `go/http` 标准。
- 移除所有 `panic`，通过 `error` 返回和日志记录确保系统稳定性。
