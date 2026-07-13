# cube-kit

[English](#english) | [中文](#cube-kit-中文说明)

`cube-kit` 为 Cube 服务提供可组合的基础设施 Mod：Redis、MongoDB、NATS/JetStream、etcd、运维 HTTP、分布式锁、配置热更、实体远程访问和统计日志等。它依赖 `cube-core` 的应用生命周期和 capability 抽象。

## cube-kit 中文说明

### 仓库关系

```text
业务服务 (cube 或自定义服务)
  -> cube-kit: 具体基础设施 Mod
  -> cube-core: Mod 生命周期、Registry 和稳定接口
```

一个 Mod 负责读取配置、构造客户端、注册 capability、接入健康检查并在停服时释放资源。业务代码应通过 `app.Registry` 取得 core 中定义的接口，而不应依赖 Mod 的私有实现。

### 安装

发布版本可用后：

```bash
go get github.com/tjbdwanghaibo/cube-kit@latest
```

本地联调三仓库时，在共同父目录创建工作区：

```bash
cd /path/to/workspace
go work init ./cube ./cube-core ./cube-kit
```

不要把仅用于本机目录结构的 `go.work` 提交到任何一个仓库。

### Mod 生命周期

所有 Mod 实现 `cube-core/app.Mod`：

```text
Init:    读取配置并构造对象
Provide: 注册 capability 到 app.Registry
Start:   建立连接、注册 health、启动后台任务
Stop:    停止后台任务并关闭连接
```

`Init` 不应启动 goroutine 或访问远端；`Provide` 不应覆盖已有 capability；`Start` 失败必须向上传递；`Stop` 必须能让停服流程收敛。

### 模块索引

| 目录 | 提供的能力 |
| --- | --- |
| `redis` | Redis client、CAS、pipeline、pub/sub、分布式锁辅助 |
| `mongo` | MongoDB client、database、collection 与 session 支持 |
| `nats` | NATS client、RPC、JetStream、Bus 与订阅管理 |
| `etcd` | 服务注册/发现、lease 保活、选主和 watcher |
| `ops` | 运维 HTTP 入口，承载 health、ready、metrics 与 admin 接入 |
| `lock` | 基于 core 锁抽象的 Mod 装配 |
| `sync` | NATS/JetStream 数据同步 transport |
| `remote_entity` | 跨服务 entity 路由、加载、同步与版本锁 |
| `configdata` | 配置加载、快照发布和热更辅助 |
| `statslog` | 周期性运行时统计日志 |
| `mods` | 可复用 Mod capability 名称常量 |

### 最小装配

以下示例选择 Redis、MongoDB 和 etcd；只注册服务实际需要的 Mod。`service` 是实现 `app.Service` 的业务入口。

```go
import (
    "errors"

    "github.com/tjbdwanghaibo/cube-core/app"
    redisapi "github.com/tjbdwanghaibo/cube-core/redis"
    "github.com/tjbdwanghaibo/cube-kit/etcd"
    "github.com/tjbdwanghaibo/cube-kit/mongo"
    "github.com/tjbdwanghaibo/cube-kit/mods"
    "github.com/tjbdwanghaibo/cube-kit/redis"
)

application := app.New("game", "v1.0.0").
    Mods(
        redis.NewRedisMod(),
        mongo.NewMongoMod(),
        etcd.NewEtcdMod(),
    ).
    RegisterServer("game", service)

if err := application.Execute(); err != nil {
    panic(err)
}
```

业务侧通过 capability key 和 core 接口读取依赖。key 常量集中在 `mods` 包，避免手写字符串散落在业务中：

```go
import (
    "errors"

    "github.com/tjbdwanghaibo/cube-core/app"
    redisapi "github.com/tjbdwanghaibo/cube-core/redis"
    "github.com/tjbdwanghaibo/cube-kit/mods"
)

client, ok := app.Lookup[redisapi.IRedis](registry, mods.ModRedis)
if !ok {
    return errors.New("redis capability is unavailable")
}
```

这里的 `redisapi.IRedis` 表示 `cube-core/redis` 中的接口类型；应用不需要也不应访问 `RedisMod` 的内部字段。

### 基础配置

默认地址适合本机开发，生产环境必须通过服务配置显式指定地址、认证、超时和 namespace。

```yaml
redis:
  addr: "127.0.0.1:6379"
  password: ""

mongo:
  uri: "mongodb://127.0.0.1:27017"

nats:
  url: "nats://127.0.0.1:4222"
  prefix: "cube"

etcd:
  endpoints: "127.0.0.1:2379"
  service_prefix: "/cube/services"
  lease_ttl: 10
```

配置键由各 Mod 的 `Init` 阶段读取。修改 NATS 可靠投递、etcd 注册重试、Mongo 连接池或 remote entity 同步策略时，请先阅读对应包源码与测试，避免在业务包中复制连接逻辑。

### 依赖关系与启停规则

- 需要 Redis 的可靠 NATS bus，必须同时注册 `redis.NewRedisMod()` 与 `nats.NewNatsMod(...)`。
- etcd 服务注册依赖有效的 `sid`、`server_type` 和 `etcd.advertise_addr` 等服务运行信息。
- 使用 `remote_entity` 或 `sync` 前，应先确认 NATS/JetStream 与服务路由已完成装配。
- 外部依赖不可用时，应通过 health/ready 暴露故障，而不是在生产环境静默降级。
- 后台 consumer、watcher、ticker 必须在 Mod 停止时退出；业务停服时先停止接入，再 flush 和关闭依赖。

### 开发与验证

```bash
go test ./redis ./mongo ./nats ./etcd
go test ./remote_entity ./sync ./ops ./statslog
go test ./...
```

新增 Mod 时，请同时提供：配置读取、`Registry` capability、health、必要的指标、确定的 Stop 行为以及不依赖真实外部服务的测试替身。

### 许可证

本仓库随 Cube 项目以 [MIT License](LICENSE) 发布。

## English

`cube-kit` contains composable infrastructure Mods for Cube services: Redis, MongoDB, NATS/JetStream, etcd, operational HTTP endpoints, synchronization, remote entities, configuration data, and runtime statistics. It builds on [`cube-core`](https://github.com/tjbdwanghaibo/cube-core) and registers typed capabilities through `app.Registry`.

```bash
go get github.com/tjbdwanghaibo/cube-kit@latest
```

Use only the Mods required by a service, keep concrete client details out of gameplay code, and create a local Go workspace for coordinated development of `cube`, `cube-core`, and `cube-kit`.
