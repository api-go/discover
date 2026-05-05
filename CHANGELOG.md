# CHANGELOG

## v1.0.3 (2026-05-05)
- 架构深度优化：将 HTTP 客户端连接池（`appClientPools`）移入 `Discoverer` 实例，实现完全的资源隔离。
- 并发安全增强：引入读写锁保护 `Config` 结构，防止高并发下的配置读写冲突。
- 生命周期管理优化：使用 `atomic.Bool` 管理 `daemonRunning` 状态，确保线程安全。
- 资源回收机制：在 `Stop()` 方法中新增 HTTP 连接池清理逻辑（调用 `Destroy` 释放闲置连接），防止内存与句柄泄漏。
- 接口严谨性：将 `Discoverer` 内部字段（如 `config`）设为私有，通过 `GetConfig`/`SetConfig` 统一访问。

## v1.0.2
- 架构重构：支持多 Discoverer 实例，消灭包级全局状态。
- 兼容性：保留包级 API 转发至 `DefaultDiscoverer`。

## v1.0.1
- 优化代码规范：修复变量名冲突，改进命名语义。
- 性能优化：优化 `AppClient` 类型，减少寻址开销。
- 故障隔离：实现本地隔离机制，不再篡改全局 Redis 状态。
- 压力缓解：心跳间隔优化至 5 秒。

## v1.0.0
- 初始版本：从 `ssgo/discover` 迁移并重构。
