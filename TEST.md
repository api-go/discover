# Test Report

## 测试场景
1. **基础发现与调用**: 验证服务启动后能自动注册到 Redis，并能通过 Caller 正确发起请求。
2. **实时同步**: 验证通过 Redis PUBLISH 更新节点信息后，客户端能实时感知并更新本地节点列表。
3. **故障剔除**: 验证当节点调用持续失败时，能自动从本地列表中剔除。
4. **环境变量配置**: 验证 `EasyStart` 结合环境变量的启动流程。
5. **负载均衡性能**: 验证默认负载均衡器在多节点集合上的选择性能与内存分配。

## 测试结果
- **Unit Tests**: `go test -v ./...`
  - `TestDiscover`: PASS
  - `TestEasyStart`: PASS
- **Module Graph**: `go list -m all` 通过，不再出现 checksum mismatch。
- **v1.5.7 Module Graph**: 使用 `redis v1.5.11` 与 `redigo v1.9.3`，不再包含撤回版本。
- **v1.5.8 Module Graph**: `apigo.cc/go/*` 基础依赖已对齐模块索引中的当前 patch 版本。

## Benchmark
- `BenchmarkDefaultLoadBalancer-16`: `32.35 ns/op`，`0 B/op`，`0 allocs/op`（Apple M3 Max）。
