# Discover SDK 发布记录与流程

用户于 2026-10-10 已确认开始发布。本次多语言 SDK 仅随 Discover 仓库 tag 发布源码，不上传包仓库。

此前要求：全部验收通过，并由用户确认可以提交之后，再统一进行版本对齐、提交和发布。在此之前不得 commit/tag/push，不提前升级依赖版本。

发布前重新读取 SOP/提交代码.md 和 .profile 中英文公开索引及 PRIVATE.md，核对远端已发布 tag，不沿用开发时缓存的版本。

从已经修改的 Log 开始梳理反向依赖图：先完成 Log 的版本确认/发布，再按拓扑顺序逐模块升级所有受影响的依赖方；每次只操作一个模块。Discover 发布后，继续升级 Service、Gateway 及其他直接/间接依赖方。具体清单以届时实际 go.mod 扫描为准，不能只升级这两个已知模块。

每个模块完成依赖对齐、GOWORK=off 普通测试/benchmark/必要集成及 race、CHANGELOG/TEST/README/.profile 更新；Patch +1，中文 commit，tag 和 push 按 SOP 执行。非 Go SDK 的包版本和发布产物独立管理。所有模块完成后再统一提交 .profile。

当前开发基线发现：Discover 使用 cast v1.5.7/log v1.5.15/redis v1.5.18，索引分别为 v1.5.8/v1.5.17/v1.5.19。此处只记录差异，不提前升级；最终发布前必须再次核对。

Service 已修改 handler.go 并增加 discover_sdk_test.go：自动健康接口和可信 IP 解析后的元数据补齐。开发验证使用只包含 discover/service 的临时工作区，其他依赖来自已发布 go.mod。Service 发布必须先使用新 Discover tag 升级依赖，最后以 GOWORK=off 重跑发布验证。不得直接提交一个依赖旧 Discover、但源码引用新 API 的 Service 版本。
