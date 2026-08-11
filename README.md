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

### etcd 多服本地镜像

`etcd.NewLocalMirror` 将一个 etcd prefix 的 revision 快照和后续 watch 映射为进程内强类型结构。初始化 watch 从快照 `Revision+1` 开始，断线或 compact 后重新获取完整快照，因此不会留下 Get/Watch 间隙。读取结果经过 `Clone`，调用方修改嵌套 map/slice 不会与 watch 更新产生数据竞争。

```go
type SharedRule struct {
    Enabled bool              `json:"enabled"`
    Params  map[string]string `json:"params"`
}

cfg := etcd.JSONLocalMirrorConfig[SharedRule]("/cube/shared-rules/")
mirror, err := etcd.NewLocalMirror(ctx, etcdClient, cfg)
if err != nil {
    return err
}
defer mirror.Close()

if err := mirror.WaitForSync(ctx); err != nil {
    return err
}
entry, ok, err := mirror.GetEntry("/cube/shared-rules/battle")
if err == nil && ok {
    next := entry.Value
    next.Enabled = true
    updated, err := mirror.PublishIfRevision(ctx, entry.Key, entry.ModRevision, next)
    // updated=false 表示其他服务器已经先更新，应重新读取后再决定是否重试。
    _, _ = updated, err
}
```

`Publish/Delete` 是 last-write-wins；需要防止多服覆盖时使用 `PublishIfRevision/DeleteIfRevision`。发布成功后本地值仍以 watch 回流为准，不在调用端提前修改，以确保所有服务器观察到相同的 etcd revision 顺序。`Status().Synced=false` 时已有数据可能陈旧，业务应根据 `LastError` 决定拒绝请求或降级。

### 开发与验证

```bash
go test ./redis ./mongo ./nats ./etcd
go test ./remote_entity ./sync ./ops ./statslog
go test ./...
```

新增 Mod 时，请同时提供：配置读取、`Registry` capability、health、必要的指标、确定的 Stop 行为以及不依赖真实外部服务的测试替身。

### 实时帧复制 Transport

`replication` 包承接 `cube-core/replication` 之上的通用网络发送策略：

- `AsyncTransport` 为每个 Session 建立独立的 latest-only Datagram Lane 和有界 Reliable Lane。
- 同一 Frame 的全部应用层分片原子入队，新 Frame 只会整体替换旧 Frame。
- `QUICTransport` 同时提供 QUIC DATAGRAM 和长度帧 Reliable Stream，是默认生产选择。
- `KCPTransport` 使用加密 + FEC 的 OOB 通道发送 Snapshot，以 KCP 字节流发送长度帧可靠消息。
- `UDPTransport` 只提供带 per-session AES-GCM 与防重放窗口的 Datagram；它没有可靠 Lane。
- `CompositeTransport` 可把受保护 UDP 与已有可靠连接组合为 core Transport。
- Session 注册和移除由 core Replicator 自动传递，房间业务不需要维护第二份网络队列生命周期。
- 默认拒绝缺片、重复片、混合 Frame 和 checksum 错误的 Datagram batch；仅在可信上游已完成等价校验时才允许开启 opaque 模式。
- Session 移除后必须等待 draining 完成才能复用 ID；可靠发送失败会终止该 Session 的两条 Lane，防止后续消息越序。
- `Close` 支持并发和重复等待，不会为每次超时调用遗留 WaitGroup waiter。

协议连接必须先经过网关鉴权并调用具体 Transport 的 `BindSession`，再调用 `FrameReplicationManager.RegisterSession`；后者会把 Session 生命周期自动传递到底层。推荐装配方式：

```go
protocol := replication.NewQUICTransport(replication.QUICTransportConfig{})
async, err := replication.NewAsyncTransport(protocol, replication.DefaultAsyncTransportConfig())
if err != nil {
    return err
}
if err := manager.SetTransport(async); err != nil {
    return err
}

// TLS/登录票据认证完成后：
if err := protocol.BindSession(sessionID, quicConnection); err != nil {
    return err
}
if err := manager.RegisterSession(corereplication.SessionInfo{ID: sessionID, OwnerID: playerID}); err != nil {
    return err
}
```

生产环境应配置非阻塞错误回调、发送超时和容量，并监控 active/draining、pending/in-flight、dropped frame、reliable abandoned/backpressure、send/receive/auth errors。UDP/KCP 仍需接入层提供登录握手、密钥派生与轮换、Cookie/限速和 DDoS 防护；QUIC 必须使用正式证书校验和固定 ALPN。完整约束见 `cube/docs/frame-replication-design.md`。

### 许可证

本仓库随 Cube 项目以 [MIT License](LICENSE) 发布。

## English

`cube-kit` contains composable infrastructure Mods for Cube services: Redis, MongoDB, NATS/JetStream, etcd, operational HTTP endpoints, synchronization, remote entities, configuration data, and runtime statistics. It builds on [`cube-core`](https://github.com/tjbdwanghaibo/cube-core) and registers typed capabilities through `app.Registry`.

```bash
go get github.com/tjbdwanghaibo/cube-kit@latest
```

Use only the Mods required by a service, keep concrete client details out of gameplay code, and create a local Go workspace for coordinated development of `cube`, `cube-core`, and `cube-kit`.
