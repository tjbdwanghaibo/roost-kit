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
| `nest` | 实例化 Nest Engine 的 App Mod，向启动层提供可注入 `nest.Client` |
| `gateway` | 玩家接入层的限流、超时和 panic 隔离中间件 |
| `sync` | NATS/JetStream 数据同步 transport；房间 Subject 订阅、20Hz 合帧和可靠 retiring |
| `spatial` | 通用整数网格坐标、并发 Terrain、ID-only AOI BlockIndex 与 A* 寻路 |
| `remote_entity` | 跨服务 entity 原子事务、不可变快照、outbox 与版本锁 |
| `saga` | 跨事务域 Saga 的 Mongo 状态/outbox、lease fencing、JetStream transport 与幂等步骤 inbox |
| `configdata` | 配置加载、快照发布和热更辅助 |
| `statslog` | 周期性运行时统计日志 |

### Saga

`saga.NewMod(definitions...)` 提供 `*core/saga.Engine`。Saga 状态、outbox 和
completion receipt 使用 MongoDB 事务保持一致；worker 通过带索引的
`next_run_at`/`next_attempt_at` 批量领取任务，并用 lease token 防止过期实例提交。

业务 Nest handler 内使用 `core/saga.EmitStart`，不要直接调用 `StartSaga`。启动意图
会和本次 Entity mutation 写入同一个 Nest WAL record；Saga Mod 的共享 durable
consumer 从 `ROOST_EFFECTS` 消费 `roost.effect.saga.start` 并幂等创建 Saga。
`StartSaga` 只用于 durable consumer、管理和恢复路径。同一 business key 如果 payload
或 deadline 不同会返回 `ErrIdentityConflict`，不会误判为重复成功。

步骤服务使用 `saga.NewMongoCommandInbox` 和 `saga.SubscribeStep`。handler 收到的
context 已绑定 MongoDB transaction；handler 的数据库修改和本次 `CommandID` receipt 会一起
提交。结果发布失败时 JetStream 会重新投递，inbox 不会再次执行业务修改，只会
重放已经保存的 completion。新的 Saga attempt 使用新的 `CommandID`，因此允许重新执行
handler；handler 必须使用稳定的 `IdempotencyKey` 查询或继续同一笔业务操作。
handler 只能通过该 context 修改 MongoDB；Mongo transaction callback 可能重试，网络调用或
其他不可逆副作用必须先写入同事务 outbox，不能直接在 handler 内执行。
步骤成功或终止后会写入 operation tombstone，并原子删除该 operation 仍在 outbox
中的旧 attempt；已经并发发布的旧 attempt/result 也只会被确认，不会进入 DLQ。

```go
mod := saga.NewMod(alliancerally.Definition())

inbox, _ := saga.NewMongoCommandInbox(mongoClient, "game", "")
sub, err := saga.SubscribeStep(ctx, jetStream, mod.Transport(), inbox,
    saga.StepConsumerConfig{
        Stream: "ROOST_SAGA", Durable: "world-create-march",
        Topic: "alliance_rally.create_march",
    }, handleCreateMarch)
```

生产环境至少监控 `Engine.Stats()` 中的 store/worker failure、conflict、publish failure 和 manual required，
并为 `ManualRequired` 提供查询、修复后 `Resume` 的管理入口。

### Nest 与玩家接入装配

`nest.NewMod(getter)` 创建并持有一个 Core Nest Engine；它不会设置旧的
`nest.Nest` 全局变量。Service 在 `Init` 阶段从 Registry 获取 `nest.Client`，再构造
codegen 生成的 Sender：

```go
nestMod := kitnest.NewMod(entityGetter)
app.Mods(nestMod)

client := app.MustLookup[corenest.Client](registry, mods.ModNest)
bagSender := bagsender.NewBagSender(client)
```

可配置项为 `nest.worker_num`、`nest.heartbeat_worker_num`、
`nest.queue_capacity`、`nest.tick_duration` 和 `nest.request_timeout`。接入端建议组合
Core `gateway.RequireAuthenticated` 与 Kit `gateway.RateLimit`、`Timeout`、`Recover`。
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

Checkpoint Mod 的 Redis WAL 默认执行强制 AOF admission，不能关闭：

```yaml
checkpoint:
  wal:
    durable_timeout: 5s
    aof_timeout: 3s
    aof_replicas: 1
```

Redis 必须为 7.2+ 并开启 AOF。Kit 会在启动阶段用同一物理连接执行探针写入和 `WAITAOF`，local fsync 固定要求 1，`aof_replicas` 决定还需多少副本确认；任一阈值未满足即启动失败。当前生产接入支持单主或 Sentinel，拒绝无法保证 Lua 与 `WAITAOF` 同连接同分片的 Redis Cluster。

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

需要在值变化时主动更新派生状态时，直接订阅镜像，不需要业务自行消费 watcher channel：

```go
subscription, err := coreetcd.SubscribeLocalMirror(mirror, ctx, func(ctx context.Context, change coreetcd.LocalMirrorChange[SharedRule]) error {
    switch change.Type {
    case coreetcd.LocalMirrorSnapshot:
        // 首次订阅和断线重同步都会进入这里；应原子替换派生状态。
        replaceRules(change.Snapshot, change.Revision)
    case coreetcd.LocalMirrorPut:
        updateRule(change.Key, change.Entry.Value, change.Revision)
    case coreetcd.LocalMirrorDelete:
        removeRule(change.Key, change.Revision)
    }
    return nil
}, coreetcd.LocalMirrorSubscribeOptions{QueueCapacity: 256})
if err != nil {
    return err
}
defer subscription.Close()
```

每个订阅按 revision 串行回调，且回调不持有 Mirror 锁；`Entry/Previous/Snapshot` 都是该回调独享的深拷贝。队列满时只关闭慢订阅并令 `subscription.Err()` 返回 `ErrMirrorSubscriberSlow`，Mirror 与其他订阅继续工作。生产服务应监控 `Done()` 与 `Err()` 并按“重新订阅后以首个 Snapshot 覆盖派生状态”的方式恢复。阻塞型回调必须响应传入的 `ctx`；停服有期限时使用 `CloseWithContext`。

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

### Observer-free 房间状态同步

`sync.RoomReplication` 承接 core 的 `SubjectSyncState` 与 `SubscriptionCoordinator`，适合 AOI、开房间和有限 LOD/权限视图：

- Entity 只保存 dirty、内容版本和 Packer，不保存 Observer。
- 同一 Profile 的冻结 Payload 在全部 Subscriber 之间共享。
- Room Frame 与每个 Subscriber 的 Session Sequence 独立推进；下游拒绝整批时二者都不跳号。
- `RoomTransportSink` 将 Snapshot/Leave 放入可靠 lane、将 Delta 分片放入 latest-only datagram lane；object ref 与 baseline 按 `(room, session)` 隔离，共享 sink 使用分片房间锁，不会因一个房间的 admission 阻塞所有房间。
- `Start(ctx, 0)` 默认按 50ms（20Hz）合并 dirty；`FlushSubject`/`FlushDirty` 可主动刷新。
- `RetireSubject` 在可靠 Leave 全部接纳前保留 Subject，并由 worker 自动重试。
- `ReliableRoomFrameSink` 必须满足 all-or-none admission；不可将会部分入队、但只返回单个成功标志的发送循环直接作为下游。

```go
room, err := sync.NewRoomReplication(roomID, frameSink)
if err != nil {
    return err
}
if err := room.RegisterSubject(subjectState); err != nil {
    return err
}
if err := room.Start(ctx, 50*time.Millisecond); err != nil {
    return err
}
defer room.Close(shutdownCtx)

_, err = room.Subscribe(ctx, subscriber, subjectState.SubjectID(), entity.SyncProfile{
    Key: "near", LOD: 1, SchemaVersion: 3,
})
```

开房间服务应使用 `RoomManager`，而不是在业务层维护无上限的 map。Manager 对房间数、全局 subject/subscriber 和空闲生命周期做统一 admission：

```go
rooms, err := sync.NewRoomManager(sync.RoomManagerConfig{
    Downstream: frameSink,
    MaxRooms: 2000,
    MaxTotalSubjects: 200000,
    MaxTotalSubscribers: 100000,
    MaxSubjectsPerRoom: 100,
    MaxSubscribersPerRoom: 100,
    ReplicationInterval: 50 * time.Millisecond,
    IdleTTL: 5 * time.Minute,
})
if err != nil { return err }
if err := rooms.Start(ctx); err != nil { return err }
defer rooms.Close(shutdownCtx)

room, err := rooms.GetOrCreate(roomID)
```

生产网络桥使用 `sync.NewRoomTransportSink`，其下游必须实现
`replication.AtomicBatchTransport`（通常为 `AsyncTransport`）。同一批中只有所有
session 都有容量时才提交 Room sequence/dirty；Session resolver 必须绑定到已认证且
已完成 `BindSession` 的连接。客户端用 `DecodeRoomWireFrame` 和
`DecodeRoomSubjectUpdate` 解码，并在 gap/checksum/baseline 错误时请求可靠 full resync。
每个 20Hz tick 会把该房间全部 dirty subject 通过一次批量 admission 合并为接收者级全局帧；不要在业务层逐 entity 循环调用主动 flush，只有确实要求立即可见的单个 subject 才调用 `FlushSubject`。

生产监控至少包含 `AdmittedFrames`、`AdmittedEntries`、`FailedBatches`、`FlushFailures`、`PendingSubjects`、`PendingRetirements`、`CallbackPending`、`CallbackCoalesced`、`CallbackPanics`、房间/subject/subscriber 使用率和 `LastError`。`PendingRetirements` 或 `CallbackPending` 持续增长应影响 readiness；慢客户端生命周期事件使用合并邮箱，不会因通知通道满而丢失。停服最后一次 flush 失败会由 `Close` 返回，共享 `RoomTransportSink` 在应用停止时也必须调用 `Close(ctx)`。

### Nest transaction WAL 与 outbox

`nestwal` 为 core Nest transaction 提供分段 commit WAL、group commit、CRC 恢复、顺序 replay、双槽 ack、transactional outbox 和主动 flush。生成 DAO 的 mutation 可通过 `CheckpointApplier` 复用现有 `checkpoint.StorageBackend`，effect publisher 必须按 `Effect.ID` 幂等。

```go
walOpts := nestwal.DefaultOptions("./data/nest-wal")
walOpts.OnFatal = stopAndFenceProcess

runtime, err := nestwal.OpenRuntime(
    walOpts,
    nestwal.CheckpointApplier{Backend: backend},
    publisher,
    nestwal.DefaultCommitterOptions(),
)
if err != nil {
    return err
}

nestMod := nestkit.NewMod(getter, runtime.NestOption())
```

`strict` handler 在返回成功前等待 group fsync；`async` 等待 write 并由后台周期刷盘。WAL replay 只在 entity guard 解锁后开始，避免与既有 release checkpoint 并发。停服应先停止接流并排空 Nest，再调用 `runtime.Shutdown(ctx)`。`ErrCommitIndeterminate` 是必须 fencing 并重启恢复的终止性错误，不能在线 rollback 后继续服务。

### 许可证

本仓库随 Cube 项目以 [MIT License](LICENSE) 发布。

## English

`cube-kit` contains composable infrastructure Mods for Cube services: Redis, MongoDB, NATS/JetStream, etcd, operational HTTP endpoints, synchronization, remote entities, configuration data, and runtime statistics. It builds on [`cube-core`](https://github.com/tjbdwanghaibo/cube-core) and registers typed capabilities through `app.Registry`.

```bash
go get github.com/tjbdwanghaibo/cube-kit@latest
```

Use only the Mods required by a service, keep concrete client details out of gameplay code, and create a local Go workspace for coordinated development of `cube`, `cube-core`, and `cube-kit`.
