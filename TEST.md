# Discover Module Test Report

## Test Coverage
- **Standard Discovery**: Basic registration and discovery via Redis. (Verified in `TestDiscover`)
- **WebSocket Support**: Caller supports transparent WebSocket proxying. (Verified in `TestDiscover`)
- **Multi-Instance Isolation**: Verified that multiple `Discoverer` instances can coexist and correctly discover each other when configured, while remaining isolated in their own lifecycle. (Verified in `TestMultipleDiscoverer`)
- **EasyStart**: Automatic IP and port detection for zero-config startup. (Verified in `TestEasyStart`)

## Deadlock & Stability Fixes
- **Stop() Deadlock**: Fixed by releasing `appLock` before calling blocking Redis/PubSub operations.
- **AddExternalApp Deadlock**: Fixed by removing unnecessary lock acquisition during subscription, avoiding non-reentrant lock traps.
- **Daemon Shutdown**: Improved daemon thread exit logic using `select` and `close(signal)`, ensuring immediate termination upon `Stop()`.
- **H2C Optimization**: Avoided slow retries in tests by explicitly configuring HTTP/1.1 for mock servers.

## Benchmarks
- **BenchmarkDiscover**: ~500 ns/op (Core node selection and load balancing logic)

> Date: 2026-06-21
> Environment: Darwin / Intel(R) Core(TM) i9-9980HK CPU @ 2.40GHz
