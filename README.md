# @go/discover

> **Maintainer Statement:** 本项目完全由 AI 维护。任何改动均遵循代码质量与性能的最佳实践。

`@go/discover` 是一个**无状态、参数驱动**的极简服务发现与负载均衡组件。它基于 Redis 实现，专注于消除微服务调用间的摩擦，并原生支持 Header 链路透传。

## 🎯 设计哲学

- **纯粹无状态 (Stateless)**：模块自身不读取配置文件，不依赖任何特定框架的上下文。配置加载由调用方负责，参数通过入口函数注入。
- **面向对象隔离**：支持多实例共存。可以在同一个进程中同时连接不同的注册中心，实现复杂的网关分发。
- **内存安全与高性能**：访问令牌 (Token) 强制受 `@go/safe` 内存保护；调用耗时由 `@go/timer` 追踪；网络层支持 H2C (HTTP/2 Cleartext)。

## 📦 安装

```bash
go get apigo.cc/go/discover
```

---

## 🛠 API Reference

### 1. 核心构造函数 (Entry Points)

#### Start: 服务端模式
在注册中心登记当前节点。
- **原型**: `func Start(registry, app, addr string, logger *log.Logger, confs ...Config) *Discoverer`
- **参数**:
    - `registry`: 注册中心地址。支持 Redis URL (如 `redis://127.0.0.1:6379/15`) 或 `@go/redis` 下定义的 Redis 配置键名。
    - `app`: 当前应用名称。
    - `addr`: 当前节点外部可访问的地址 (如 `192.168.1.10:8080`)。
    - `logger`: 必填。建议传入带有 TraceID 的 Logger 以确保链路可追踪。允许传 `nil` (回退至 `log.DefaultLogger`)。
    - `confs`: 可选。传递 `discover.Config` 结构体进行精细化配置。

#### Open: 纯客户端模式
仅用于调用其他服务。
- **原型**: `func Open(registry string, logger *log.Logger, confs ...Config) *Discoverer`

### 2. Discoverer 实例方法 (RPC 调用)

所有的业务调用均应通过 `Start` 或 `Open` 返回的 `*Discoverer` 实例进行。

#### 基础 HTTP 调用
返回 `*gohttp.Result`，可结合 `go/http.To[T]` 实现结果绑定。
- `func (d *Discoverer) Get(app, path string, headers ...string) *gohttp.Result`
- `func (d *Discoverer) Post(app, path string, data any, headers ...string) *gohttp.Result`
- `func (d *Discoverer) Put(app, path string, data any, headers ...string) *gohttp.Result`
- `func (d *Discoverer) Delete(app, path string, data any, headers ...string) *gohttp.Result`
- `func (d *Discoverer) Head(app, path string, headers ...string) *gohttp.Result`
- `func (d *Discoverer) Do(method, app, path string, data any, headers ...string) *gohttp.Result`

#### 链路透传调用 (Context Propagation)
通过 `From(r)` 提取原始请求上下文（TraceID, UserID 等）并向后透传。
- **原型**: `func (d *Discoverer) From(request *http.Request) *Caller`
- **示例**: `res := d.From(r).Post("user-service", "/create", reqData)`

#### WebSocket 支持
- **原型**: `func (d *Discoverer) Open(app, path string, headers ...string) *websocket.Conn`

#### 实例生命周期
- `func (d *Discoverer) Stop()`: 优雅停止心跳、注销节点并释放内部连接池。

### 3. 配置结构 (Strongly Typed Config)

#### Config: 发现器配置
```go
type Config struct {
    Weight         int                    // 节点权重 (默认 100)
    Calls          map[string]CallConfig  // 依赖服务的调用配置
    CallRetryTimes int                    // 下游节点的最大重试次数 (默认 10)
}
```

#### CallConfig: 下游服务调用配置
```go
type CallConfig struct {
    Timeout time.Duration  // 超时时间
    Token   *safe.SafeBuf  // 访问凭据 (强制安全存储，防止内存泄露)
    Http2   bool           // 是否强制使用 HTTP/2 (H2C)
    SSL     bool           // 是否使用 HTTPS/WSS 协议
}
```

---

## 💡 最佳实践示例

### 标准服务端启动
```go
import (
    "apigo.cc/go/discover"
    "apigo.cc/go/log"
)

// 准备安全令牌
token := safe.NewSafeBuf([]byte("secure-app-token"))

d := discover.Start(
    "redis://127.0.0.1:6379/15",
    "user-service",
    "192.168.1.10:8080",
    logger,
    discover.Config{
        Calls: map[string]discover.CallConfig{
            "auth-service": { Timeout: time.Second, Token: token },
        },
    },
)
defer d.Stop()

// 调用并自动解析
res := d.Get("auth-service", "/api/verify")
user, err := http.To[User](res)
```
