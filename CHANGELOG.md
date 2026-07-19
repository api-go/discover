# CHANGELOG

## [v1.5.8] - 2026-07-19
- **Redis API 对齐**: 服务注册、心跳、注销和节点拉取统一改用 `redis v1.5.11` 类型化命令，减少通用结果转换。
- **依赖升级**: 对齐 `cast`、`config`、`crypto`、`encoding`、`file`、`http`、`id`、`log`、`rand`、`safe`、`shell` 的当前 patch 版本。
- **文档与测试**: 补齐全局配置和负载均衡 API 说明，新增默认负载均衡器 Benchmark。

## [v1.5.7] - 2026-07-18
- **依赖校验修复**: 升级 `redis` 至 `v1.5.11`，并将间接依赖 `redigo` 固定为稳定版 `v1.9.3`，移除撤回版本。

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
