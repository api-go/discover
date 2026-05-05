# CHANGELOG

## v1.0.1 (2026-05-05)
- 优化代码规范：修复变量名冲突（Shadowing），改进 `tryTimes` -> `attempts` 等语义命名。
- 性能优化：优化 `AppClient` 中的 `Data` 和 `Headers` 类型（从指针改为直接引用），减少内存寻址开销。
- 性能优化：优化 `NextWithNode` 中的切片分配。
- 架构优化：导出 `log.FillBase`，支持外部模块实现高效自定义日志。
- 功能增强：引入 `DiscoverLog`，实现基于对象池的高性能发现过程日志记录。
- 标准对齐：统一使用 `apigo.cc/go/http` 中定义的 Header 常量。
- 文档完善：为所有导出类型和方法添加详细文档注释。
- 测试增强：添加 `BenchmarkDiscover` 基准测试。

## v1.0.0
- 从 `ssgo/discover` 迁移至 `apigo.cc/go/discover`。
- 采用全新的 `apigo.cc/go` 基础设施（log, redis, http, cast, u）。
- 优化了注册中心同步机制，使用 `redis.Subscribe` 简化 PubSub 处理。
- 增强了负载均衡算法，引入更精确的得分计算。
- 统一了 Header 定义，对齐 `go/http` 标准。
- 移除所有 `panic`，通过 `error` 返回和日志记录确保系统稳定性。
