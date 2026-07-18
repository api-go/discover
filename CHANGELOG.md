# CHANGELOG

## [v1.5.6] - 2026-07-18
- **依赖升级**: 升级全部 `apigo.cc/go/*` 依赖到当前版本，移除已废弃的 `convert` 间接依赖。
- **Checksum 修复**: 淘汰内容已变化的 `crypto/safe v1.0.4` 依赖记录，重新生成可信的 `go.sum`。

## v1.0.0
- 从 `ssgo/discover` 迁移至 `apigo.cc/go/discover`。
- 采用全新的 `apigo.cc/go` 基础设施（log, redis, http, cast, u）。
- 优化了注册中心同步机制，使用 `redis.Subscribe` 简化 PubSub 处理。
- 增强了负载均衡算法，引入更精确的得分计算。
- 统一了 Header 定义，对齐 `go/http` 标准。
- 移除所有 `panic`，通过 `error` 返回和日志记录确保系统稳定性。
