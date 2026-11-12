# Test Report

## v1.5.17 验证
- `TestDynamicSubscriptionsDuringRestart` 使用 Redis DB15 验证重启期间并发新增订阅与停止；本地节点夹具独立于注册中心。
- 验证握手取消及时返回，既不增加也不清零已有失败计数；默认 Open 不共享应用 CookieJar，多 Header 参数保留重复值。
- 2026-10-14：GOWORK=off 全量（DISCOVER_REDIS_INTEGRATION=1）、基准和 Open/订阅/生命周期 race（2 次）通过；Apple M3 Max，DefaultLoadBalancer 15.30 ns/op，0 B/op、0 allocs/op。API 使用 go doc -all 与源码核对。
- Open 覆盖 200/301/400/401/403/404/426/429/500 不重试且不累计节点失败；503 切换及最终响应保留。
- 覆盖多值头/Cookie、路由、服务令牌、子协议、握手 Set-Cookie、握手超时后连接仍可用、取消不计失败以及 WSS 私有 CA。
- GOWORK=off 使用已发布 http v1.5.12；API 使用 go doc 与源码核对。

## v1.5.15 发布验证（2026-10-06）
- 普通测试、benchmark 与启用 Redis 的完整 race 检查通过。
- `TestHeartbeatStopWaitAndRestart` 覆盖 20 轮启动/停止、重复 Start、3 个并发 Wait、重复 Wait 与注销结果；无定时 sleep。
- `BenchmarkDefaultLoadBalancer`: 31.84 ns/op，0 B/op，0 allocs/op（Apple M3 Max）。

## v1.5.14 验证
- Redis 集成场景通过 `DISCOVER_REDIS_INTEGRATION=1` 显式启用，默认测试不依赖外部 Redis。
- `go test -v ./...` 与 `go test -bench=. ./...` 通过。

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
- **v1.5.12 Module Graph**: 使用 `redis v1.5.11` 与 `redigo v1.9.3`，不再包含撤回版本。
- **v1.5.8 Module Graph**: `github.com/api-go/*` 基础依赖已对齐模块索引中的当前 patch 版本。
- **v1.5.9 Module Graph**: `log v1.5.11`、`redis v1.5.12` 与 `shell v1.5.6` 通过依赖审计。

## Benchmark
- `BenchmarkDefaultLoadBalancer-16`: `32.35 ns/op`，`0 B/op`，`0 allocs/op`（Apple M3 Max）。

## SDK 开发验证（2026-10-10，未发布）

- `GOWORK=off go test -v ./...`：通过。
- `DISCOVER_REDIS_INTEGRATION=1 GOWORK=off go test -race -v ./...`：通过；Redis 注册/订阅、停止/重启、并发调用、取消及流式响应所有权。
- `GOWORK=off go test -bench=. ./...`：通过；DefaultLoadBalancer 15.19 ns/op、0 B/op、0 allocs/op（Apple M3 Max）。
- 新增用例覆盖健康默认/自定义/405、原生/旧 Caller 的多值应用头、POST body 换节点重放、连续失败清零、501、并发、取消、本地非法 Header、节点超时、原生/旧流式应答在返回后可读。
- API 核对：工具 getExports 未提供，使用 GOWORK=off go doc 与导出源码交叉核对。

### 各语言与实际互通

- Python 3.14：2 个 unittest 通过，包含 WSGI 延迟迭代时的认证上下文与请求 ID 隔离。
- Node.js：npm test 通过，包含 POST 换节点、连续失败清零、501、Header、健康及 Redis 生命周期。
- Java 17：Maven/JUnit 通过，包含同类 HTTP/健康用例及真实 Redis。
- PHP 7.4：常驻/FPM 独立入口的 Redis/HTTP/H2C 验证通过；PHP/Ruby 源码语法检查通过，Composer manifest 验证通过。
- 跨语言 runner：Python、Node.js、Java、PHP 的 HTTP/H2C/WS 到 Go 通过；Ruby HTTP/WS 到 Go 通过；Go 经 Redis 发现并调用 Python WSGI、Node.js、Java HttpExchange、Ruby Rack host、PHP Swoole 的固定健康路径通过。
- runner 检查自身注册/注销和心跳清理，释放自己的进程及前缀键；没有停止用户已有 Redis。

### 验证范围

本轮没有执行 Redis 断线/进程故障注入、真实 FPM master、TLS/WSS 证书场景或 Spring/FastAPI/Laravel/Rails 自动生命周期装配验收。Ruby 默认 H2C/WSS 为明确不支持；非 Go ManualDoWithNode 流式对应接口尚未提供。详见 sdk/README.md 的能力表，不将这些场景标记通过。

Service 入口集成已在仅 discover/service 的临时工作区通过完整 race、benchmark 及针对性元数据/健康检查；其 GOWORK=off 发布验证留到 Discover 发布并升级依赖后进行。发布及版本对齐尚未执行。

## v1.5.16 发布验证（2026-10-10）

- GOWORK=off go test -v ./... 与 go test -bench=. ./... 通过，依赖均为已发布 tag。
- API 以 GOWORK=off go doc 与导出源码交叉核对（getExports 工具不可用）。
- GOWORK=off go test -race ./... 通过。

```text
BenchmarkDefaultLoadBalancer-16    	79286206	        15.42 ns/op	       0 B/op	       0 allocs/op
```

- v1.5.16 依赖升级后重新运行 Python/Node.js/Java 测试及完整双向跨语言 runner，全部通过；本次仅发布 SDK 源码，真实 FPM/TLS 与具体框架装配仍为文档列明的后续范围。

## External context (2026-10-23)
- Prepared native ingress, immutable normalized address metadata, raw legacy context isolation, no default Origin relay, unchanged RPC target.
- API inspection: go doc plus exported source review in place of unavailable getExports.

- Temporary replace and real development Gateway HTTPS validation passed before release. Released dependency baseline: HTTP v1.5.15; GOWORK=off full suite and benchmarks, no replacements.

- Released-dependency benchmark on Apple M3 Max: BenchmarkDefaultLoadBalancer 15.46 ns/op, 0 B/op, 0 allocs/op.

## v1.5.19 default relay validation
- Default policy without proxy addresses preserves incoming Request-ID browser context; real internal RPC target Host, HTTP transport, service token and absent default Origin are checked end to end.
- 开发验收使用 GOWORK=off 与显式 ../http replace；发布复验改用正式 HTTP v1.5.16。

- Development validation passed: GOWORK=off go test -v ./..., go test -bench=. ./..., go test -race ./... and go vet ./.... Apple M3 Max: Load balancer 15.50 ns/op, 0 B/op, 0 allocs/op. HTTP is tested independently; downstream modules use documented explicit local replace mappings.

- v1.5.19 发布复验：HTTP v1.5.16 正式依赖，GOWORK=off、无 replace；全量测试及 benchmark 通过，API 使用 go doc -all 与导出源码核对（getExports 不可用）。

## v1.5.20 发布验证（2026-10-26）

GOWORK=off，正式依赖 crypto v1.5.9，无 replace。`go test -v ./...`、`go test -bench=. ./...`、`go vet ./...`、`go mod verify` 通过。使用 `go doc` 与源码交叉核对 API，替代 getExports。现有回归覆盖本模块业务；新旧 GCM 格式、认证失败和性能基准见 crypto v1.5.9。

跳过的外部条件场景：
- --- SKIP: TestDiscover (0.00s)
- --- SKIP: TestEasyStart (0.00s)
- --- SKIP: TestHeartbeatStopWaitAndRestart (0.00s)
- --- SKIP: TestDynamicSubscriptionsDuringRestart (0.00s)

基准结果：
```text
BenchmarkDefaultLoadBalancer-16    	77819749	        15.32 ns/op	       0 B/op	       0 allocs/op
```

## Recovery supervisor — 2026-10-31 (unreleased)

- GOWORK=off; explicit development replacement: github.com/api-go/redis => ../redis. All other dependencies remain published tags. Start means lifecycle startup; readiness is checked separately.
- TestRegistrationRecovery: isolated Redis delayed startup, registration/health and node snapshot recovery, server loss/restart, Stop/Wait while Redis is absent, no re-registration after Stop.
- TestStartWithUnresponsiveRedis: a TCP server accepts connections but never answers commands; initial Start returns within the test's 200 ms bound and Stop completes within four seconds. These are test thresholds, not production SLA guarantees.
- TestCallFailuresDoNotDeleteGlobalRegistration: legacy HTTP, native HTTP and WS failures at threshold one preserve global membership; a cached node can serve requests again after backend recovery.
- GOWORK=off full verbose tests, Redis-enabled race, benchmarks and go vet passed. DefaultLoadBalancer on Apple M3 Max: 14.85 ns/op, 0 B/op, 0 allocs/op (development run).
- API inspected with GOWORK=off go doc -all and exported source because getExports is unavailable.
- Cross-language recovery runner: sdk/conformance/recovery.py. Go, Python, Node.js, Java, Ruby and resident PHP pass startup without Redis, repeated retries, missed notification reconciliation, outage exceeding the 10-second lease, empty Redis restart, Stop with Redis available/absent, and no late re-registration. Peers also assert local call failure does not delete a global membership record. PHP-FPM verifies request initialization without Redis and later request recovery without owning registration.
- Cross-language HTTP/H2C/WS and health runner: sdk/conformance/run.py. Native Go ↔ Python/Node/Java/Ruby/PHP interoperability and lifecycle cleanup pass. Ruby retains its documented H2C/WSS limitations. Python and Node unit suites and Java/JUnit pass.
- Runtime validation: local Redis, Python 3.14 in an isolated venv, Node, Java 17, Ruby 2.6 with redis 4.8.1, PHP 7.4 with Redis/Swoole. No existing Redis process is stopped. Isolated fault-injection Redis processes disable persistence and are cleaned up.
- Service validates actual HTTP listening during Redis absence, 503→200 recovery and owner shutdown. Gateway's existing full race suite passes with a temporary modfile replacing only Redis, Discover and Service with these local modules; no Gateway source/dependency file is changed.
- Verification scope: fault duration exceeds the lease but is not a 24-hour soak. No live Learning deployment, production ACL/failover, Redis Cluster or real FPM master validation is claimed. Lua EVAL and its underlying commands must be permitted.

## GitHub migration validation (pending release)

Module paths and internal dependency versions are migrated. Development verification uses GOWORK=off and explicit sibling replacements; remove these replacements before release. See migration validation logs for actual results. Existing unrelated working changes are preserved and require separate review.

API inventory verification: `GOWORK=off go doc` succeeded; output retained in the migration validation directory. Source exports were preserved by the path migration.

## GitHub release verification

GOWORK=off go test -v ./... and go test -bench=. ./... passed using published GitHub dependencies, without local replacements. go mod verify passed.

## 调用日志验证 — 2026-11-12

CallLog 的 RequestID 续传、query 排除、单次摘要委托开关；HTTP/流式/WebSocket 现有完整回归与基准。开发使用 /tmp/apigo-validation-discover.mod 显式替换本次 timer/log 等本地模块，GOWORK=off。API 用 go doc 与导出源码核对。
