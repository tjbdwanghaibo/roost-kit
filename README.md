# cube-kit

`cube-kit`（仓库目录名 `roost-kit`，Go 模块 `github.com/tjbdwanghaibo/cube-kit`）是 roost 框架的**中间件组件层**：它把 Redis、MongoDB、NATS/JetStream、etcd、本地磁盘 WAL 等具体基础设施实现为 `cube-core` 定义的稳定接口与 `app.Mod` 生命周期组件，业务服务只需按需装配。

```text
业务服务（游戏服 / world 服 / 自定义服务）
  └─> cube-kit   具体基础设施 Mod（本仓库）
        └─> cube-core   Mod 生命周期、app.Registry、稳定接口与 Nest 执行引擎
```

---

## 1. 组件总览

| 组件 | 解决什么问题 | 外部设施 | 何时用 |
| --- | --- | --- | --- |
| `nestwal/` | Nest 事务的段式 commit WAL：group commit、崩溃恢复、顺序 replay、transactional outbox；是 `DurabilityStrict/Pipelined` 的 `TransactionCommitter` 实现 | 本地磁盘 + NATS JetStream（effect 投递）+ MongoDB（经 checkpoint backend 落库） | 任何要求"成功回包 = 已持久化"的 Nest 服务 |
| `checkpoint/` | 实体快照异步落库：journal、批量 flush、版本 CAS、admission 重试，以及 pipelined durable watermark 外化闸门 | MongoDB + Redis（7.2+，强制 AOF 快照 WAL） | 使用 Nest 实体自动持久化的服务 |
| `nest/` | 装配一个实例级 core Nest 引擎，注册 `nest.Client` capability | 无（依赖 `nestwal` Mod） | 所有 Nest 服务 |
| `redis/` | Redis 客户端、pipeline、pub/sub、分布式锁（`SetNX`）与 `AutoExtendLock` 自动续期包装 | Redis | 缓存、去重、可容忍双写的互斥 |
| `mongo/` | MongoDB 客户端、collection、session/事务封装 | MongoDB | 一切持久化 |
| `nats/` | NATS 连接、RPC、JetStream、可靠 Bus | NATS/JetStream | 服务间消息 |
| `etcd/` | 服务注册/发现、`IFencedElection` 选主（CreateRevision 栅栏）、prefix 本地镜像 | etcd | 多实例部署的发现与选主 |
| `remote_entity/` | 跨服务实体的原子事务、不可变快照分发、`IVersionedLock`（栅栏 + 版本）| Redis + NATS（sync）+ MongoDB | 跨服实体所有权与远程提交 |
| `saga/` | 跨事务域长事务：Mongo 状态机 + outbox + lease fencing + 幂等步骤 inbox | MongoDB + NATS JetStream | 跨服务多步业务流程 |
| `sync/`、`syncstream/` | 房间/AOI 状态同步：20Hz 合帧、可靠 retire、分片、压缩 | NATS/JetStream 或 `replication` transport | 实时房间同步 |
| `replication/` | 帧复制网络层：QUIC/KCP/UDP transport，UDP 为 per-session AEAD 加密 + 防重放 | 无（自带网络协议栈） | 实时帧下发（客户端连接） |
| `gateway/` | 接入层中间件：限流、超时、panic 隔离 | 无 | 玩家接入链路 |
| `spatial/` | 整数网格地形、四方向 A* 寻路、ID-only AOI 块索引（`QueryRadius`）。**non-goals：无 Z 轴/navmesh/内建兴趣管理**（见包注释） | 无 | 房间级 2D 场景的寻路与 AOI |
| `ai/`、`taskflow/` | 泛型行为树 + 策略控制器；任务编排 Runner | 无 | NPC/玩法逻辑 |
| `lock/`、`ops/`、`configdata/`、`statslog/` | 进程内锁管理器；运维 HTTP（health/ready/metrics）；配置快照热更；周期统计日志 | —/HTTP/本地文件 | 通用运行时设施 |
| `mods/` | 全部 capability 名称常量（`mods.ModRedis`、`mods.ModNestWAL`…） | 无 | 业务从 Registry 取依赖时使用 |

---

## 2. 快速启动

### 2.1 独立体验 nestwal：写入 → 崩溃 → 恢复重放

下面的程序不需要任何外部设施，直接演示 WAL 的核心承诺：**Append 返回即持久化；崩溃产生的 torn tail 会在重开时被截断；重放从 ack fence 开始且不重复**。

新建一个空目录，写入 `go.mod`（发布版可直接 `go get`；本地联调时用 `replace` 指向你的 checkout）：

```go
module nestwal-demo

go 1.25.0

require (
	github.com/tjbdwanghaibo/cube-core v1.6.2
	github.com/tjbdwanghaibo/cube-kit v1.6.1
)

// 本地联调时（路径改成你的实际 checkout 位置）：
replace github.com/tjbdwanghaibo/cube-kit => /path/to/roost-kit

replace github.com/tjbdwanghaibo/cube-core => /path/to/roost-core
```

`main.go`：

```go
// nestwal 独立演示：写入 → 模拟崩溃（torn tail）→ 恢复重放 → ack 检查点。
// 运行：go run .
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	corenest "github.com/tjbdwanghaibo/cube-core/nest"
	"github.com/tjbdwanghaibo/cube-kit/nestwal"
)

func record(id byte, entityID int64, payload string) corenest.CommitRecord {
	var txID corenest.TransactionID
	txID[15] = id
	return corenest.CommitRecord{
		ID:         txID,
		Handler:    "demo.add_gold",
		CreatedAt:  time.Now().UnixNano(),
		Durability: corenest.DurabilityStrict,
		Mutations: []corenest.EntityMutation{{
			EntityID: entityID, Resource: "player", Codec: "json",
			Data: []byte(payload),
		}},
		Effects: []corenest.Effect{{
			ID: fmt.Sprintf("demo-effect-%d", id), Topic: "demo.gold_changed",
			Payload: []byte(payload),
		}},
	}
}

func main() {
	dir, err := os.MkdirTemp("", "nestwal-demo-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ctx := context.Background()

	opts := nestwal.DefaultOptions(dir)
	opts.OnFatal = func(err error) { log.Fatalf("WAL fatal, fence the process: %v", err) }

	// ---- 第一次“进程生命周期”：写入三笔事务后模拟断电 ----
	wal, err := nestwal.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	for i := byte(1); i <= 3; i++ {
		fence, err := wal.Append(ctx, record(i, int64(i), fmt.Sprintf(`{"gold":%d}`, int(i)*100)))
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("append tx#%d -> fence{segment:%d offset:%d}\n", i, fence.Segment, fence.Offset)
	}
	if err := wal.Close(ctx); err != nil {
		log.Fatal(err)
	}
	// 模拟崩溃：向活动段尾部追加半个 frame（断电时常见的 torn write）。
	segment := filepath.Join(dir, "segment-00000000000000000001.wal")
	f, err := os.OpenFile(segment, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := f.Write([]byte{0x52, 0x53, 0x57}); err != nil { // 不完整的 magic
		log.Fatal(err)
	}
	_ = f.Close()

	// ---- 第二次“进程生命周期”：重开时自动截断 torn tail，从 ack fence 重放 ----
	wal, err = nestwal.Open(opts) // Open 内部 scanFramesEnd 截断损坏尾部
	if err != nil {
		log.Fatal(err)
	}
	defer wal.Close(ctx)

	applied := map[int64]string{}
	var last corenest.CommitFence
	replay := func(fence corenest.CommitFence, rec corenest.CommitRecord) error {
		for _, m := range rec.Mutations {
			applied[m.EntityID] = string(m.Data) // 幂等落库（演示用内存表）
		}
		for _, e := range rec.Effects {
			fmt.Printf("replay tx=%s publish effect %s\n", rec.ID, e.ID)
		}
		last = fence
		return nil
	}
	if err := wal.Replay(ctx, replay); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("recovered entities: %v\n", applied)

	// 落库+发布成功后推进 ack 检查点（双 slot + generation，fsync 持久化）。
	if err := wal.Ack(ctx, last); err != nil {
		log.Fatal(err)
	}

	// 再次 Replay：扫描从 ack fence 开始，已确认前缀不再出现。
	replayed := 0
	if err := wal.Replay(ctx, func(corenest.CommitFence, corenest.CommitRecord) error {
		replayed++
		return nil
	}); err != nil {
		log.Fatal(err)
	}
	fmt.Printf("records after ack: %d (expect 0)\n", replayed)

	// ---- Pipelined 提交：Enqueue 拿 ticket，group commit 后水位线先行发布 ----
	ticket, err := wal.Enqueue(ctx, record(9, 9, `{"gold":900}`))
	if err != nil {
		log.Fatal(err)
	}
	<-ticket.Done() // 组提交 fsync 完成即被唤醒
	if err := ticket.Err(); err != nil {
		log.Fatal(err) // 只可能是 ErrCommitIndeterminate
	}
	fmt.Printf("ticket lsn=%d durable, watermark DurableLSN=%d\n", ticket.LSN(), wal.DurableLSN())
}
```

预期输出（已实际编译运行验证）：

```text
append tx#1 -> fence{segment:1 offset:206}
append tx#2 -> fence{segment:1 offset:412}
append tx#3 -> fence{segment:1 offset:618}
replay tx=00000000000000000000000000000001 publish effect demo-effect-1
replay tx=00000000000000000000000000000002 publish effect demo-effect-2
replay tx=00000000000000000000000000000003 publish effect demo-effect-3
recovered entities: map[1:{"gold":100} 2:{"gold":200} 3:{"gold":300}]
records after ack: 0 (expect 0)
ticket lsn=1 durable, watermark DurableLSN=1
```

> 生产环境不要直接使用裸 `WAL`：应通过 `nestwal.NewMod` / `nestwal.OpenRuntime` 拿到 `Committer`，由它实现 `corenest.TransactionCommitter` 并负责后台重放、落库、outbox 投递与 ack。

### 2.2 作为 committer 接入 cube-core Nest（生产装配）

生产装配走 Mod 体系。下面的骨架已通过编译校验（`app.Service` 与 `entity.Getter/ManagerAccess` 由业务提供）：

```go
import (
	"github.com/tjbdwanghaibo/cube-core/app"
	"github.com/tjbdwanghaibo/cube-core/bus"
	corenest "github.com/tjbdwanghaibo/cube-core/nest"
	kitcheckpoint "github.com/tjbdwanghaibo/cube-kit/checkpoint"
	kitnest "github.com/tjbdwanghaibo/cube-kit/nest"
	"github.com/tjbdwanghaibo/cube-kit/mongo"
	"github.com/tjbdwanghaibo/cube-kit/nats"
	"github.com/tjbdwanghaibo/cube-kit/nestwal"
	"github.com/tjbdwanghaibo/cube-kit/ops"
	"github.com/tjbdwanghaibo/cube-kit/redis"
)

walMod := nestwal.NewMod(false) // true 时额外接入 remote_entity

application := app.New("game", "v1.0.0").
	Mods(
		ops.NewOpsMod(), // health/ready/metrics HTTP
		redis.NewRedisMod(),
		mongo.NewMongoMod(),
		nats.NewNatsMod(bus.JSONCodec{}),
		kitcheckpoint.NewMod(kitcheckpoint.WithEntityAccess(access)),
		walMod,
		kitnest.NewMod(getter, func(o *corenest.NestOpts) {
			// nest Mod 的 Provide 晚于 nestwal Mod 执行（DependsOn 保证），
			// 此时 NestOptions() 已携带 committer 与 pipelined 灰度配置。
			for _, opt := range walMod.NestOptions() {
				opt(o)
			}
		}),
	).
	RegisterServer("game", service)

if err := application.Execute(); err != nil {
	panic(err)
}
```

对应的最小配置（各键在各 Mod 的 `Init` 中读取）：

```yaml
sid: 1
redis:
  addr: "127.0.0.1:6379"
mongo:
  uri: "mongodb://127.0.0.1:27017"
nats:
  url: "nats://127.0.0.1:4222"

nest:
  wal:
    dir: "data/wal/nest/1"     # 缺省为 data/wal/nest/<sid>
    group_commit_interval: 10ms
  pipelined:
    allowlist: ["player.consume", "player.reward"]  # 允许 DurabilityPipelined 的 handler
    async: false                                    # Phase 2：异步完成
```

handler 侧通过 `HandlerMeta{Durability: corenest.DurabilityStrict}`（或 `DurabilityPipelined`）声明持久化级别；成功回包时事务已在 WAL 中持久化，落库与 effect 投递由 `nestwal.Committer` 的后台重放循环异步完成。

---

## 3. 核心概念

### 3.1 Mod 装配与配置体系

所有组件实现 `cube-core/app.Mod` 四阶段生命周期：

```text
Init(cfg)   读取 viper 配置、构造参数（不启 goroutine、不访问远端）
Provide(r)  构造对象并向 app.Registry 注册 capability
Start()     建连、注册 health、启动后台任务（失败必须向上传递）
Stop()      停后台任务、flush、关连接（保证停服收敛）
```

- **依赖声明**：Mod 通过 `DependsOn()` 声明拓扑（如 `nestwal` 依赖 `checkpoint`、`nats.jetstream`、`health`；`nest` 依赖 `nest.wal`），框架按拓扑序执行 Provide/Start。
- **capability 查询**：业务永远通过 `app.Lookup[接口类型](registry, mods.ModXxx)` 取依赖，只依赖 `cube-core` 接口，不触碰 Mod 私有实现。名称常量集中在 `mods/name.go`。
- **配置**：每个 Mod 在 `Init` 中读取自己的配置命名空间（`nest.wal.*`、`checkpoint.*`、`nest.pipelined.*`…），零配置时使用可运行的默认值。`nestwal/mod.go` 的 `Init` 是完整样例：所有 `nest.wal.*` 键逐个覆盖 `DefaultOptions`。

### 3.2 Durability 管线全景（nest 事务 → 磁盘 → 数据库）

一笔 Nest 事务从 handler 返回到最终落库经过如下阶段（Strict 与 Pipelined 只在前两步不同）：

```text
handler 成功返回
  │  持实体锁
  ├─ Strict:    Committer.Commit → WAL.Append —— 在锁内等待本批 fsync 完成
  ├─ Pipelined: Committer.Enqueue → WAL.Enqueue —— 锁内只做“可拒绝检查+拿 LSN+入队”，
  │             立即解锁；返回 CommitTicket
  ▼
group commit（nestwal/wal.go writerLoop）
  批量聚合（BatchDelay/BatchMaxRecords/BatchMaxBytes）→ 一次 write + fsync
  ▼
durable 水位线发布（resolveDurableLocked）
  DurableLSN 先推进，随后才唤醒 ticket 等待者
  ▼
外化闸门（externalization gates）
  一切“离开进程”的动作 gate 在 durableLSN >= tx.LSN：
  · 回包 / AfterCommit 副作用（cube-core nest 引擎内）
  · checkpoint 快照外化（checkpoint/mod.go entityDurable）
  ▼
后台重放循环（nestwal/committer.go replayPass）
  从 ack fence 顺序 Replay → MongoAtomicApplier 落库（幂等）→ EffectPublisher 发 outbox
  ▼
ack 检查点推进（nestwal/checkpoint.go）
  双 slot + generation + fsync；已确认前缀此后不再重放，旧 segment 可回收
```

关键点：**WAL 是 commit point**。落库（Mongo）和 effect（JetStream）都是 commit 之后的至少一次投递，靠幂等（版本 CAS、`Effect.ID` 去重、`MongoEffectInbox` 收据）收敛为恰好一次的业务效果。`checkpoint` 的快照落库则永远不允许跑到 WAL 前面——这是 watermark 闸门的职责。

设计文档：`cube-core` 仓库的 `NEST_TRANSACTION_WAL.md` 与 `NEST_PIPELINED_COMMIT.md`（中文，含正确性论证）。

---

## 4. 关键实现细节

每条都给出源文件指引，读代码时可对照。

### nestwal：WAL 与提交

- **双 CRC frame**（`nestwal/wal.go` `encodeFrame`/`scanFramesFrom`）：20 字节 frame 头含 payload CRC32 和头自身 CRC32。头 CRC 保证长度字段可信（否则一个损坏的 length 会让扫描越界误判），payload CRC 保证内容可信。
- **torn-tail 截断**（`wal.go` `openActive` → `scanFramesEnd`）：重开时以 `allowTornTail=true` 扫到最后一个完整 frame，`Truncate` 掉尾部残缺字节——断电时最后一笔未 fsync 的写入被安全丢弃，且只可能丢“未向调用方确认”的后缀。
- **group commit**（`wal.go` `writerLoop`/`collectBatch`/`processBatch`）：单写线程聚批，一次 `write` + 一次 `fsync` 摊薄毫秒级 fsync 成本；`GroupCommitInterval` 定时器兜底刷新异步写。
- **Enqueue ticket 与唯一拒绝点**（`wal.go` `Enqueue`）：Pipelined 的 `Enqueue` 在实体锁内被调用，因此把**一切可拒绝检查**（编码、大小、容量预约 `reservedBytes`、terminal 状态、队列准入）同步做完；`enqueueMu` 让 LSN 分配与入队原子，保证 LSN 序 = 物理日志序。入队成功后调用方即可解锁——之后唯一可能的失败是 ticket 上的 `ErrCommitIndeterminate`。`processBatch` 对已预约请求**永不拒绝**（调用方已凭准入放弃了回滚权）。
- **为什么 watermark 必须先于唤醒发布**（`wal.go` `resolveDurableLocked`）：ticket resolve 是一个承诺——“`DurableLSN` 已覆盖你的记录”。外化闸门在 `Done()` 触发后立刻读水位线，若先 `close(done)` 再推水位线，等待者会读到旧水位线而再次阻塞甚至误判未持久化。因此先 `durableLSN.Store(upto)`，再逐个 `close(ticket.done)`。
- **ack fence 是合法扫描起点**（`wal.go` `Replay`）：ack fence 记录的是某个 frame 的**结束偏移**，天然落在 frame 边界上，所以重放可以跳过已确认的 segment 和 fence 所在 segment 的已确认前缀（`scanFramesFrom(file, segment, ack.Offset, ...)`），不必每轮从头读全量并重新校验 CRC。
- **ack 检查点：双 slot + generation + 目录 fsync**（`nestwal/checkpoint.go`）：`ack-0.chk`/`ack-1.chk` 按 `generation & 1` 交替写，写入走 tmp 文件 → fsync → rename → `syncDirectory`。加载时取 CRC 合法者中 generation 最大的一个：任何时刻允许一个 slot 是 torn 的，另一个 slot 在替换者持久化前不会被碰。ack 丢失最多导致重复重放（幂等吸收），绝不会跳过记录。
- **fsync 不确定 ⇒ 熔断而非重试**（`wal.go` `Options.OnFatal` 注释、`setTerminal`；`nestwal/mod.go` `onFatal`）：fsync 报错后，内核可能已经丢弃了 dirty page 却清掉了错误标记，重试的 fsync 会“成功”但数据并没有落盘——写入结果从此不可知。所以任何物理写/fsync 失败都被包装为 `corenest.ErrCommitIndeterminate` 并置 terminal：拒绝一切后续写入、以该错误 resolve 所有 pending ticket，并经 `OnFatal` 熔断进程（Mod 会 `NestMgr.Fence` + `app.RuntimeFailure.Fail`）。重启后由重放从最后一个 ack 恢复出唯一可信的历史。
- **单写者锁与容量健康**：`writer.lock` 文件锁防止双进程写同一目录（`lock_unix.go`）；`MaxDiskBytes`/`MaxUnackedAge` 超限时 `Healthy()` 报错，接入 health 后表现为实例不健康而不是静默膨胀。
- **落库与 outbox**（`nestwal/committer.go`、`mongo_atomic_applier.go`、`effect_inbox.go`）：重放循环在事务仍被实体锁持有时让路（`errTransactionHeld`，由 `TransactionReleased` 唤醒）；`MongoAtomicApplier` 把普通 after-image 与 Remote Entity 提交放进**一个 Mongo session 事务**；消费端用 `MongoEffectInbox` 把业务写与 effect 收据同事务提交实现恰好一次，JetStream 的 `MsgID` 去重窗口只是热路径优化。

### checkpoint：快照外化闸门

- **durable watermark gate**（`checkpoint/mod.go` `entityDurable`/`onEntityRelease`）：实体释放时若 `base.LastCommitLSN() > DurableLSN()`，说明它最后一笔 pipelined 提交还没落盘——此时**先不做快照**（保住 dirty 位），放进 `pendingReleases`，由 admission 重试循环在水位线追上后再快照提交。闸门由 `nestwal/mod.go` 的 `cp.SetDurableWatermark(runtime.Committer.DurableLSN)` 自动接线；未用 pipelined 的实体 LSN 为 0 恒通过，代价只是一次原子读。
- **pendingReleases / pendingSaves 与熔断**：重试集合有容量上限（`checkpoint.admission_pending_capacity`），超限触发 `fenceAdmission` → `RuntimeFailure`——宁可停服也不静默丢快照。
- **Stop 有 30s 上界**（`mod.go` `Stop`）：若 WAL 已被熔断，水位线永远追不上，无界 flush 会让停服挂死；带 deadline 的 `StopWithContext` 保证停服收敛。
- **Redis 快照 WAL 强制 AOF**（`mod.go` `validateRedisWALDurability`）：启动时用同一物理连接做探针写 + `WAITAOF`，阈值不满足直接启动失败；要求 Redis 7.2+ 开 AOF，拒绝无法保证 Lua 与 `WAITAOF` 同连接同分片的 Redis Cluster。

### 分布式锁与选主

**先做二选一**（两套锁并存是刻意的分层，不是重复实现）：

| 需求 | 用哪个 | 原因 |
| --- | --- | --- |
| 可容忍偶发双执行的互斥（缓存预热、可去重任务、优化性串行化） | `redis.IDistLock`（可套 `AutoExtendLock`） | 轻量；但**无栅栏**——TTL 过期后旧持有者不自知，存在双执行窗口 |
| 正确性互斥（实体所有权、存储必须能拒绝旧持有者的写） | `remote_entity` 的 `versionedLock` / `etcd.IFencedElection` | fence 计数器独立于 TTL 永不回退，下游按 fence 单调性 CAS 拒旧 |

判据一句话：**如果"锁过期后旧持有者又写了一笔"会造成数据损坏，就必须用带 fence 的那套**；`redis/lock.go` 的包注释里写有同样的契约边界。

- **`AutoExtendLock` 的 TTL 预算重试**（`redis/lock.go` `watch`）：每次续期调用都被限时（`extendCtx` 超时 = 续期间隔），防止挂死的 Redis 冻结 watchdog 而租约静默过期。续期遇到**瞬时错误**（网络/超时）不立即判丢——上一次成功续期可能仍覆盖着租约——只有 `time.Since(lastRenewed) >= ttl`（租约可证明已过期）才置 `Err()`；而服务器明确答复“不再持有”则立即停止续期。注意它**不带栅栏**：需要防旧持有者脏写时用 `IVersionedLock`。
- **`versionedLock` 的 fence 与 TTL 分离**（`remote_entity/versioned_lock_lua.go`）：fence 来自独立的 `key:fence` 计数器（`INCR`），**永不过期、不共享锁 hash 的 TTL**——若 fence 随锁一起过期，计数器归零后新持有者会拿到更小的 fence，栅栏失效。锁本体是带 TTL 的 hash（owner/version），下游写路径按 fence 单调性做 CAS 拒绝旧持有者。
- **幂等 unlock**（`versioned_lock_lua.go` `versionedUnlockLua`）：unlock 的应答可能丢失，重试时若发现 `version == 本次要写的新版本`，证明先前那次 unlock 已生效，返回 2（成功）而非 NotOwned——依赖“unlock 版本按 key 单调唯一”的接口契约。
- **watchdog 续期上限 2×TTL**（`remote_entity/versioned_lock.go` `Touch`）：`AutoAsyncTouch` 每次把剩余 PTTL 加 `AsyncTouchExtend`，但封顶 `2*TTL`——续期只能维持租约，不能把租约无限抬长，持有者崩溃后锁仍在有界时间内自然释放。
- **选主 fence**（`etcd/election.go`）：`Fence()` 返回本候选者 campaign key 的 **CreateRevision**，它随 prefix 的每次领导权更替单调递增。`IsLeader()` 存在固有 stale 窗口（lease 已在服务端过期、客户端未感知），所以领导权敏感写必须携带 fence token 并在存储侧拒绝旧 token。实现上 fence 先于 `isLeader` 标志发布：观察到 `IsLeader==true` 的调用方一定能读到本任期的 token。

### 其它

- **saga Mongo store**（`saga/mongo_store.go`）：任务领取用 `FindOneAndUpdate` 带 `version` + `lease_until <= now` 的 CAS，`$inc lease_token` 使过期实例的后续提交被 owner+token 过滤；`SaveWithOutbox` 在**一个 Mongo 事务**里同落 saga 状态、outbox 命令与 completion 收据，重复完成通过收据 digest 幂等判定。
- **replication AEAD UDP**（`replication/udp_crypto.go`、`udp_transport.go`）：AES-GCM per-session；`SendSalt`/`ReceiveSalt` 每个方向独立且构造时强制不相等（同 key 双向复用同一 nonce 空间会灾难性破坏 GCM）；nonce = salt(4B) + 单调 sequence(8B)，序列号耗尽即拒发；接收端 64 包位图防重放窗口；**地址迁移只在 AEAD 验证通过之后**（`Open` 成功且 `isCurrentRoute`）才生效，未认证的包改不了路由。
- **spatial / ai / gateway 的定位声明**（各包 package doc）：三者都是**轻量构件而非中间件服务**，选型前先读它们的 non-goals——`spatial` 是均匀网格 + 四方向 A* + ID-only 块索引（`QueryRadius` 先块粗筛再精确过滤，附块版本号供增量同步），**无 Z 轴、无 navmesh、无内建兴趣管理**；`ai` 是行为树骨架（组合/装饰节点 + 黑板 + tick 控制器），**无节点库、无编辑器格式、无规划器**；`gateway` 只是中间件集合（限流/鉴权守卫），**不是网关服务器**。房间级 2D 玩法够用；MMO 级空间服务/AI 中间件需要自建或另选。

---

## 5. 学习路径

按下面的顺序读源码与测试，每步都可 `go test ./<pkg>/` 验证认知：

1. **框架心智模型**：`cube-core` 仓库 `README.md` + `RUNTIME_EXECUTION_MODEL.md`，然后读本仓库 `mods/name.go` 和任意一个小 Mod（如 `lock/lock_mod.go`）理解四阶段生命周期。
2. **WAL 设计文档**：`cube-core/NEST_TRANSACTION_WAL.md` → `NEST_PIPELINED_COMMIT.md`（中文，含 Strict/Pipelined 语义对比与正确性论证）。
3. **nestwal 主线**（本仓库核心，建议精读）：
   - `nestwal/wal.go`（帧格式、group commit、Enqueue/ticket、terminal 熔断）→ `nestwal/checkpoint.go`（双 slot ack）→ `nestwal/committer.go`（重放循环）→ `nestwal/runtime.go` + `nestwal/mod.go`（装配与 watermark 接线）。
   - 测试按价值排序：**`crash_test.go`**（`TestNestWALCrashChildProcess` 用真实子进程 `SIGKILL` 验证崩溃后 durable 前缀完整——最值得读）、`pipelined_test.go`（ticket 语义、同步拒绝、terminal 时 fail 所有 ticket）、`wal_test.go`（torn tail、ack、rotate）、`fatal_fence_test.go`（熔断到 Nest/RuntimeFailure 的传导）、**`pilot_test.go`**（`NESTWAL_PILOT=1 go test -run TestPipelinedPilot -v ./nestwal/`：真实引擎 + 真实磁盘的 strict vs pipelined 压测，理解为什么需要 pipelined）。
4. **checkpoint 闸门**：`checkpoint/mod.go` + `checkpoint/pipelined_gate_test.go`（`TestReleaseGateDefersSnapshotUntilDurable`）+ `admission_limit_test.go`。
5. **锁与选主**：`redis/lock.go` + `lock_test.go` → `remote_entity/versioned_lock.go`、`versioned_lock_lua.go` + `versioned_lock_unlock_test.go` → `etcd/election.go` + `election_test.go`。
6. **横向扩展**：`saga/mongo_store.go`（lease CAS）→ `remote_entity/transaction_manager.go`（跨服事务追踪）→ `replication/udp_crypto.go`、`udp_transport.go` → `sync/room_replication.go` → `spatial/`、`ai/`、`taskflow/`。

---

## 6. 版本与仓库关系

- **与 cube-core 的版本对应**：以本仓库 `go.mod` 为准——每个 cube-kit 版本固定其所需的 `cube-core` 最低版本（当前 `main`/v1.6.1 之后要求 `cube-core v1.6.2`）。升级 cube-kit 时同步升级 cube-core 到 go.mod 声明的版本即可；两仓库的 tag 大体同步演进（v1.2.x … v1.6.x）。
- **仓库分工**：

  | 仓库 | 模块路径 | 角色 |
  | --- | --- | --- |
  | roost-core | `github.com/tjbdwanghaibo/cube-core` | 生命周期、Registry、Nest 引擎、稳定接口与设计文档 |
  | roost-kit（本仓库） | `github.com/tjbdwanghaibo/cube-kit` | 基础设施 Mod 与中间件实现 |
  | roost-codegen | `github.com/tjbdwanghaibo/roost-codegen` | DAO/Sender 等代码生成 |
  | roost-skill | — | 技能/战斗玩法组件 |

- **本地联调**：在共同父目录建 workspace，勿把 `go.work` 提交进任何仓库：

  ```bash
  cd /path/to/workspace
  go work init ./roost-core ./roost-kit ./your-service
  ```

- **开发验证**：

  ```bash
  go build ./... && go test ./...
  # 单独跑核心持久化链路：
  go test ./nestwal/ ./checkpoint/
  ```

新增 Mod 时请同时提供：配置读取、Registry capability、health 检查、确定的 Stop 行为，以及不依赖真实外部服务的测试替身。

本仓库以 [MIT License](LICENSE) 发布。
