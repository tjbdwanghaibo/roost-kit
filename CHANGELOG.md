# Changelog

本文件从 v1.6.1 起维护；更早版本见 git 历史。

## [Unreleased]

### Added
- `scripts/pretag.sh`：打 tag 之前的发布预检（tag major 与 module 路径后缀一致、
  tag 未存在、无 replace、工作区干净、`GOWORK=off` 下 build/vet/test 通过）。
  由 tag push 触发的 CI 运行在 tag 已存在之后，能报告但阻止不了。

### Added（roost-service 的三个前置能力）

- **`versionstore`：版本化状态的契约 + Redis 与内存实现。** 契约里**没有无条件写**：
  `Update` 是改变值的唯一途径、自己完成 CAS 重试，调用方无法表达"无论如何都写"。
  这是刻意的——只要契约同时提供无条件 `Set`，就会有实现满足接口而跳过检查，
  且编译器不会发现（业务仓里两个互不相关的服务各自这样丢过更新）。三条硬规则各自
  对应一个已确认的缺陷：**比较版本不比较值**（比较重新序列化的值会在滚动发布改变
  结构体序列化的那一刻卡住所有写入）；**版本由存储侧分配、按 key 单调**，跨副本与
  跨重启可比（进程内计数器不是版本）；**版本 0 表示"不存在"而非"跳过检查"**，
  已存储的值版本恒 ≥ 1，因此没有代码路径能靠留零值绕过比较。
  `Create` 是独立的仅插入路径，因为"不得覆盖"与"由当前值计算"是不同意图，用
  `Update` 表达它要依赖调用方检查 `found`——而那正是会被忘掉的检查。
  重试带**指数退避 + 全抖动**：立即重试会让竞争看起来像故障（N 个写者一起输、
  一起重试、一起耗尽预算），而把它报成冲突就得到一个与"后端不可达"无法区分的错误——
  正是这个原语要消除、而不是复现的缺陷。
- **`servicerpc`：服务间 RPC 客户端**（从业务仓提升）。总线上的类型化调用、etcd
  实例选择、lightweight/JetStream 传输选择，以及服务应答用的稳定响应状态约定。
  它是基建而非服务，业务仓与 roost-service 不应各持一份。
  新增 `KeyAffinityPicker`：同一 key 的调用固定路由到同一实例。round-robin 对无状态
  读是对的默认值，对"按 key 持锁改共享状态"是错的——它把同一逻辑 key 的连续操作发到
  不同副本，于是进程内互斥等于没有互斥（业务仓的匹配队列因此出现两个副本各自提交
  一场匹配）。亲和本身不是正确性机制（实例会增减），它消除的是让该故障变成常态的
  稳态竞争；共享状态仍需 CAS。映射按 sid 排序后计算，因此不依赖 discovery 的返回顺序。
- **`mongo/mongotest`：求值 filter 的内存 MongoDB**（原 `internal/mongofake`，导出）。
  不求值的测试替身比没有替身更糟：它让所有版本 CAS、租约谓词、唯一索引冲突和 tombstone
  守卫无条件通过，于是这些构造要保障的语义全部不受测试保护。业务仓与 roost-service
  需要与 kit 自身测试同一份替身；手写副本可靠地会出错（用 `fmt.Sprint` 比较、用
  `encoding/json` 而非 bson 编码文档、或让 `Pipeline` 返回 nil 使生产读路径根本不执行）。
  kit 内 8 个使用它的包已迁移，`go test -race` 全绿。

### Changed（破坏性：Go 模块路径改为 roost-kit，协议前缀默认值改为 roost）

- 模块路径 `github.com/tjbdwanghaibo/cube-kit` → `github.com/tjbdwanghaibo/roost-kit`，版本延续。
- NATS 主题前缀默认值：`cube.sync`/`cube.room` → `roost.sync`/`roost.room`；`nats` mod 默认前缀
  `cube` → `roost`；`statslog` 默认服务名 `cube` → `roost`。这些都是可配置默认值。
  **滚动升级**期间新旧节点若都用默认值会互相听不见，请在升级前显式配置同一前缀。
- JetStream 同步流默认名 `CUBE_SYNC` → `ROOST_SYNC`，与 `ROOST_EFFECTS`/`ROOST_SAGA` 对齐。
  流名是 broker 侧持久状态，见 roost-core CHANGELOG 同条说明。
- `syncstream` 指标名 `cube_sync_*` → `roost_sync_*`，仪表盘与告警规则需同步更新。
- `ai` 行为树 JSON schema 标识 `cube.ai/v1` → `roost.ai/v1`，不做兼容；已有行为树定义把 `"schema"` 字段改名即可。
- QUIC ALPN `cube-replication-v1` → `roost-nettransport-v1`；JetStream 去重 MessageID 前缀 `sync:` → `room:`
  （跟随包名）。两者都是握手/去重时双端必须一致的值，**不做兼容**：滚动升级期间未升级的客户端会被
  已升级服务端拒绝握手，新旧节点对同一条消息算出不同 MessageID 会各投递一次。请同批升级两端，
  或在 TLS 配置里显式固定 ALPN。

### Added
- **`manager`：`ManagerMod`**，一个 Service 的内存单例 manager（场景注册表、路由表、缓存这类有 Start/Stop 但没有自己持久状态的逻辑）的生命周期拥有者。cube-core 早就声明了契约——`app.IManager`、`app.ManagerDependencyProvider`、`app.IManagerStopperWithContext` 的注释都写着"managed by ManagerMod"——但实现一直缺失，各业务仓各写一份。行为：
  - 按 `DependsOn` 拓扑序启动、**严格逆序**停止（manager 绝不比它依赖的东西活得久）；
  - 无依赖关系的 manager 之间保持**注册序，且每次进程一致**。这里没有复用 core 的 `container.TopologicalSortCache`：它的队列由 map 播种，独立节点的顺序每进程不同，而启动顺序是可观察行为——顺序漂移会把一个必现的顺序 bug 变成偶发的；它报告环的方式也是打日志返回 nil，而启动门禁必须说出是哪个 manager 成环；
  - 启动失败回滚已启动的，**失败的那个不 Stop**——它没有启动完，Stop 就得处理半构造对象；清理是 `Start` 自己的责任。这条契约写在代码注释里并由测试钉住；
  - **启动中收到 shutdown 会中止启动**，而不是和它赛跑。否则 `Stop` 排空已启动的之后，`Start` 会继续把后面的 manager 启起来，于是 shutdown 报告成功而仍有 manager 在跑；
  - `Start` 之后 `Register` 返回 `ErrManagerRegisterAfterStart`。接受它等于加进一个永不启动也永不停止的 manager，唯一症状是很远处的一个 nil；
  - `Start` 之前没有 `Provide` 直接报错，而不是把 nil registry 发给每个 manager；
  - `Stop` 幂等，逐个 manager 都给停止机会并**汇总全部失败**（首个失败就中断会漏掉其余的）；优先使用 `IManagerStopperWithContext` 并把调用方 ctx 透传进去。
  - 指标：`manager.start.duration{manager}`（histogram）、`manager.started`（gauge）。
  - 20 条测试，含 `-race`；确定性排序、启动中止、失败不回滚失败者、`Register` 拦截四条都做过变异验证（破坏实现 → 断言精确打红）。

### Changed（测试质量）
- `spatial` 与 `ai` 的两条并发测试此前丢弃全部返回值，只靠 race 检测器——一个
  拒绝每次写入的 terrain 或一个丢写的 Blackboard 都能通过。现断言并发写后每格/
  每键保留最后一次写入（期望值由测试自己的写入计划推导，不硬编码），并断言
  `Blackboard.Snapshot()` 返回**拷贝**而非活状态别名。两条都用变异测试验证过。
- `etcd/local_mirror_test.go` 的 11 处裸通道接收改为有界的 `awaitWatchStarted`；
  `dataengine` 与 `remoteentity` 的裸接收改为 `awaitChan(t, ch, what)`。此前
  watcher 不启动会表现为挂 10 分钟后一份堆栈，现在是 5 秒内一句
  "mirror never started its etcd watch"。

### Changed（破坏性：包与标识符重命名，无行为变化）

跟随 cube-core 的命名整理，把只描述"机制"的名字换成描述"职责"的名字：

| 旧 | 新 | 说明 |
| --- | --- | --- |
| 包 `sync` | 包 `room` | 它是房间状态同步的房间侧，不是同步原语 |
| 包 `replication` | 包 `nettransport` | 它是 KCP/QUIC/UDP 网络传输，不是复制 |
| 包 `remote_entity` | 包 `remoteentity` | Go 包名不用下划线 |
| 包 `taskflow` | 包 `actionflow` | 与 core 对齐 |
| `SyncMod` | `RoomMod` | |
| `RoomReplication`（及 `New*`/`*Config`/`*Stats`/`Default*Interval`/`Err*Stopped`） | `RoomBroadcaster`（`DefaultRoomBroadcastInterval`、`ErrRoomBroadcasterStopped`） | 它每 50ms 把脏 subject 聚合成帧广播给订阅者 |
| `mods.ModSync` = `"sync"` | `mods.ModRoom` = `"room"` | |
| `mods.ModObs` = `"obs"` | `mods.ModMetrics` = `"metrics"` | 随 core |

文件重命名：`room/jetstream_sync.go` → `jetstream_syncbus.go`、`room/nats_sync.go` →
`nats_syncbus.go`、`room/room_replication.go` → `room_broadcast.go`、
`nettransport/async_transport.go` → `channel.go`、`nettransport/transport.go` →
`sender.go`、`nettransport/control.go` → `control_plane.go`、`syncstream/adapter.go` →
`publisher.go`、`ai/wire.go` → `tree_parser.go`。

**配置段兼容**：room mod 优先读 `room.*` 配置段，读不到才回退到旧的 `sync.*` 并打印
一条弃用告警，因此既有部署配置无需在升级同一时刻修改。JetStream 去重 MessageID 前缀随包名改为 `room:`（见下）。


### Fixed（独立复审 F1/F2/F3/F4/F6/F8，均带"无修复即红"验证过的回归测试）
- **`dataengine` outbox backlog 探针不再随积压等比变慢**：补 `created_at` 索引（原先 oldest 查询按未索引字段排序，探针会随它所要度量的积压一起变慢），并把探针从"每次 `RunOnce` 末尾无条件执行"（默认 2 workers × 100ms = 每秒 20 次全量计数）改为按 `dataengine.outbox.backlog_interval`（默认 1s，跨 worker 用 CAS 抢样）限流；新增 `OutboxWorker.RefreshBacklog` 供健康检查即时取样，`Mod.checkHealth` 已改用它，避免读到最多落后一个间隔的陈旧 gauge。
- **`nats`：删除 `nats.rpc.duplicate_completion` 指标与那条永不失败的断言**。实施中发现它在原理上无法有意义——`worker.Worker.safeHandle` 是 `handler(task)` **加** `defer task.OnRelease()`，所以每个任务必然两次到达 `complete()`，"二次到达"是普通路径而非异常（改成计数器后，10 万 pending 取消测试立刻报出 10 万次"重复完成"）。改为把 `sync.Once` 换成到达计数（同一保证、更直白），并用直接测试覆盖真正的属性：handler→release、仅 release（拒绝准入的路径）、16 goroutine 并发到达、nil 回调。pool 的"handler + OnRelease"所有权协议已写入注释。
- **CI 新增 `go vet -tags integration ./...`**：4 个 `//go:build integration` 文件（约 1000 行崩溃/故障切换测试）被 build tag 同时排除在 `vet`/`build`/`test ./...` 之外，可以静默腐烂而无人察觉。真正运行仍需 Mongo/NATS，入口是既有的 `scripts/integration/dataengine-env.sh`。
- **lease fence 改用 core 的共享谓词，并为未命中加上可见计数**：`leaseFencesMatch` 不再手拼 filter，改用 `coredata.LeaseFence.Predicate(now)`；`saga` 的 `claimStatusPending` 取自 `coredata.LeaseFenceStatusPending`。原先同一份 claim schema 被 `dataengine` 与 `saga` 两侧各自拼写、无任何编译期耦合，任一侧改名都会让**每个被 fence 的事务静默变成 no-op**（谓词不可满足与"租约确实过期"从内部无法区分，两者都被当作 skipped 正常提交）。新增跨包耦合测试：在 `saga` 包内用 `Reserve`/`Bind` 真实写出 claim 与 fence，再拿生产谓词查生产文档，并逐字段偏移验证谓词确实在拒绝——已验证把 `bson:"lease_token"` 改名即变红。fence 未命中现在计 `dataengine.fence.skipped.total{resource}`。
- **`dataengine` 投影批路径不再把良性重放判成致命冲突**（原缺陷会导致服务永久起不来）：单变更记录走快路径时**有意不写** transaction marker（省一次往返正是快路径的意义），但一旦 WAL ACK 丢失（Mongo 提交与 checkpoint fsync 之间崩溃，或任何 `Ack` 报错），该记录会在多记录批中重放——此时"无 marker + 精确版本 CAS 打空"与真实冲突无法区分，而 `ProjectBatch` 原先直接返回致命的 `ErrProjectionConflict`，令 projector 停摆；由于同一批每次重启都会重放，服务从此无法启动。现在批路径改为返回新的 `ErrProjectionBatchNeedsPerRecord`**延后判定**，由 `Projector.replayPass` 回退到逐条投影——单记录路径比对存储版本与 `_last_tx`，能区分"已应用的幂等重放"与"真实冲突"。真实冲突仍然致命（语义不变，只是改由有能力判定的一方来判）。
- **`dataengine` 实体聚合加载在事务重试下不再误报"数据损坏"**：`readAggregate` 的 `loaded` 累加器声明在 `ReadConsistent` 回调**之外**（`missing`/`tombstones` 却正确地在回调内重置）。`ISession.WithTransaction` 的契约明示会自动重试回调（副本集切主、snapshot 不可用、网络抖动），第二次进入时重名守卫立刻命中上一轮条目，健康数据被报成 `ErrEntityAggregateCorrupt: duplicate DAO resource`——恰好发生在最需要平稳降级的故障切换时刻。现已将 `loaded`/`remoteVector` 的重置移入回调首行。

### Changed
- 测试基建重构：新增 `internal/mongofake`——**真正求值** filter/update/唯一索引/事务回滚的内存 Mongo，取代此前 5 份各自手写的 `ICollection` 桩（约 80 个方法）。旧桩把存储建模成"返回测试预置的东西"，无法分辨正确查询与错误查询，因此投影版本 CAS、saga step 租约 CAS、command 收据去重、remote_entity 版本 CAS、effect inbox 幂等这些**靠查询条件承载正确性**的机制全部不在测试覆盖内（F1 正是其中从未被走到的一条路径）。迁移过程即刻暴露两处真实差异（`uint8` 字段的数值加宽、bson 日期的毫秒精度）。不支持的构造一律显式报错而非静默匹配。

### Added — Data Engine
- 新增统一 Data Engine runtime：WAL recovery barrier、Mongo Put/Patch/Delete 版本 CAS、transaction receipt/effect outbox、聚合 snapshot load、system-transaction migration、tombstone、健康/积压硬限制和有界 shutdown。
- Saga 新增 Data Engine step inbox（claim lease + 权威 receipt replay），Remote mutation 与普通 mutation 可在同一 Mongo transaction 投影；NATS publisher 与 WAL ACK 解耦。
- 新增 WAL/投影/Saga benchmark 矩阵及 NATS outage backlog 恢复测试，脚本位于 `scripts/perf/dataengine.sh`。

### Changed — Data Engine
- `persistence.engine` 只接受 `dataengine`；Nest 通过 Data Engine lazy proxy 获取 committer，recovery 完成后才接流量。
- WAL reader 同时支持 v1/v2；新配置默认写 v2，只有显式 `dataengine.wal.writer_version=1` 才为历史 reader 保留兼容写入。
- 旧 Checkpoint Mod 与 standalone NestWAL Mod 已物理删除；`nestwal` 仅作为 Data Engine 内部 WAL 库存在，不再参与业务 Mod 装配。

### Fixed
- Data Engine Runtime 现在为 EntityManager 注册唯一 delete admitter。本地删除用隔离 strict transaction 或当前 Nest transaction 写 tombstone；Remote 删除先准备完整 RemoteWriteBatch 并携带显式 delete intent，只有 durable admission 后才从内存移除，rollback 保持实体可用，结果不确定时 fail-stop。
- Saga Data Engine step 的 reservation 必须显式传给 `inbox.Bind(command, reservation)`。Mongo 投影在应用任何 mutation/effect 前原子校验 claim owner、lease token、command digest、`pending` 状态与未过期租约；陈旧记录只写 skipped transaction marker 后 ACK，防止旧 worker 晚到提交，也避免 poison WAL。skipped 状态在重放时同样阻断事务外 Remote publish。
- Data Engine 默认 WAL writer 修正为支持 Patch/Receipt 的 v2；聚合加载先绑定 DAO ID，并把“部分缺失/部分 tombstone”判为损坏而不是整个实体不存在；绝对 `expires_at` 索引不再重复叠加一轮 TTL。
- Data Engine：修复部分重放时的 segment ACK 正确性；重放按 WAL 顺序切分，失败 segment
  不会被后续 ACK 跨越。Mongo marker 与 WAL 存储格式保持不变。
- NATS async RPC 增加 started/completed/pending、callback latency、duplicate completion 和 callback queue rejected 指标，并加入 10 万 pending 取消守恒验收，确保 exactly-once completion 不仅被实现，也能被监控和规模验证。
- NATS async RPC 将 reply、timeout、publish error 和 Stop 收敛到唯一 `LoadAndDelete` 终态入口；callback worker 队列关闭/满载时同步兜底，并以 once 防止正常执行与释放路径重复回调，实现 exactly-once completion。
- NATS 同步 RPC 使用 `RequestWithContext`，调用方取消可立即中断正在等待的请求；默认重试策略随 core 收敛为单次发送，避免未知幂等性的业务调用被框架静默重复执行。
- Mongo、Redis、ConfigData、Lock 四个内置 Mod 补齐 `StopWithContext`，统一接受 App 的 shutdown budget；关闭错误不再被 Redis/Mongo 的无返回 Stop 路径静默吞掉。
- 多 capability Mod 在注册前统一预检名称、重复项和现有冲突，避免 Provide 失败留下半装配 registry；Etcd 服务注册复用有界启动 context，不再可能无限阻塞 Start。

### Changed
- NATS Mod 通过 core `OptionalDependsOn` 声明 Redis 可选排序依赖，Nest Mod 同样声明 Remote Entity；reliable bus/远程事务集成不再依赖业务传入 Mods 的偶然顺序。
- README 接入 Roost 三级文档导航，并修正停机说明：App 会把统一 shutdown deadline 传给 Saga/Nest/WAL 的 `StopWithContext`。

### Added
- **`robot/` 机器人框架的 kit 侧**（配合 cube-core 新增的 `robot` 框架）：
  - `RegisterKCPDialer`/`RegisterQUICDialer` 经 core `transport.RegisterDialer` 挂载 KCP（AES-GCM + FEC，复用 `replication.DialKCP`，KCP 流上直接跑统一包协议）与 QUIC（`replication.DialQUIC` + 单双向 stream 承载，对端关流归一化为 EOF）客户端拨号——runner 配置里 `Transport.Type` 选中即用。
  - `LockstepBot`：lockstep 客户端半场——基于 core `lockstep.FrameAssembler`（去重 + 严格顺序）应用帧、按帧生成并提交输入、关键帧哈希上报（缺省 `FrameHasher` 输入链 FNV 折叠：确定性模拟下输入同则状态同）、缓冲越界时每 gap 恰好一次追帧请求；出站走业务注入的 `LockstepSink`（提交/上报/追帧的线格式由业务定义）。回归基线：3 bot × 600 帧 × 各自独立 30% 丢包，冗余 + 追帧后全帧应用、`DesyncDetector` 全程零误报（16 次追帧、30 次裁决）。

### Changed
- 移除根模块的本地 `replace`。当前未发布的 robot 功能依赖 core HEAD 新增 API，因此开发主线固定到可由 Go proxy 解析的精确 pseudo-version `cube-core v1.8.1-0.20260826111010-16f057d5e22f`；正式 tag 必须先切回已发布的 core tag，release-hygiene 会拒绝 pseudo-version。

## [1.8.0] - 2026-08

### Added
- `redis`：客户端实现 `fredis.ListTrimmer`/`ListRemover`（LTRIM/LREM）——core `failurelog` 的 trim/delete 回退由此走就地操作，消除 DEL+RPUSH 两步间崩溃丢整表的窗口。（go.mod 已升级到 cube-core v1.7.1，临时 replace 已删除。）

### Fixed（lockstep v1.7.1 发布后复审，9 缺陷 + 6 权衡全部实施，均带回归测试）
- `lockstep`：**构造期传输预算校验**——`NewRoom` 拒绝"冗余深度 × 座位数 × 单输入上限 + 线格式开销 > MaxDatagramBytes（默认 1232）"的配置（原先单个合法客户端满载 payload 可让整房间每个广播包超过 UDP 上限、全部发送失败且冗余无从修复）；`RedundancyDepth` 按解码端 `MaxBroadcastFrames` 收口（原先配 100 会让所有客户端整包拒收而服务端零告警）。
- `lockstep`：**追帧与会话绑定加固**——session 独占（被其他座位/观战者占用的 session 拒绝 Attach，消除双发与追帧互踩）；同 session 重复 Attach 幂等且保留追帧游标（原先防御性重复 Attach 会静默取消追帧留下永久帧洞）；`StartCatchup` 拒绝超过下一帧的起点（原先白丢一帧实时广播）、追帧中重复调用取 min 游标；可靠通道连续失败超预算（`CatchupMaxFailures`，默认 8）自动放弃并显式报错（原先永久钉死在"不收实时也追不上"态）；历史被 Trim 出缺口时以 `ErrCatchupUnservable` 放弃。
- `lockstep`：**裁决语义重做**——quorum 改为"同意组大小"（默认由座位数派生过半），串谋少数抢先上报不再能对诚实玩家触发 `OnDesync`；`OnDesync` 按离群**集合**变化触发（原先只比基数，等基数翻转被吞）；`ReportHash` 校验座位与帧号上界并返回错误（原先可用伪造座位/未来帧制造 Trim 清不掉的无界内存）；Trim 过的帧墓碑化。
- `lockstep`：迟到折叠不再遮蔽真实输入（显式当前帧输入覆盖折入的过期 payload）；新增 `lockstep.input.rejected.total{reason}`；广播/追帧迭代确定性排序；新增观战者通道（`AttachSpectator`/`SpectatorCatchup`）与 `Room.Close()`；**文档修正：datagram 通道必须直连裸 transport，不得经 `AsyncTransport` 的 latest-only 合帧队列**。

### Fixed
- `configdata` Mod：Rollback 回调把 `configdata.version` gauge 复位到 `Old.Version`——此前 apply 后失败的 reload 会让 gauge 永久停在一个已被回滚掉的版本号上（监控盘误报 reload 成功）。

### Changed
- go.mod：`cube-core` 升至 v1.8.0（lockstep 复审修复 + configdata 自查修复 + ListTrimmer/ListRemover）。
- README 按全量能力审计重写扩充：组件总览逐包补齐（taskflow 独立成行、sync/syncstream 拆分、replication 补 AsyncTransport/三 transport 矩阵/CompositeTransport、remote_entity 补 Mod 装配与所有权状态机）；新增 §3.2 配置命名空间与跨键不变量、§3.3 capability 常量→注册者→实际类型对照表、§3.4 停机语义表；§4 新增 replication/sync 四角色/syncstream/remote_entity/saga（`SaveWithOutbox` 描述修正为 `Apply`）/基础设施细节/并发定位一览各节；修正装配顺序描述（仅 6 个 Mod 声明 DependsOn，nats(reliable)→redis 与 nest→remote_entity 是未声明的顺序依赖）与版本号引用。

## [1.7.1] - 2026-08

### Fixed（v1.7.0 发布后独立复审的 8 项发现，均带回归测试）
- `spatial`：MaxVisible 从每房间独立执行收口为 **cluster 级全局 top-N**（缝边 observer 的预算曾被相交房间数放大）；`AddRoom` 回填既有 observer 的边界镜像（静止 observer 对后加房间曾有永久可见性盲区）；拒绝 id 0（曾半跟踪导致可见性对不同 observer 分裂）；`LeaveRadius` 加上界 + 订阅盒算术全饱和（极值曾回绕致 observer 全盲）；observer 全量评估改最近优先 admission（曾产生瞬态 Enter+Leave 对）；补 gofmt。
- `ai`：guard 谓词在装配期限定为无状态形态（condition/sequence/selector/inverter，action 作谓词曾每次检查都发起动作）；nil-root 策略丢弃动作完成事件（曾无界堆积）；`Cooldown.Reset` 保留计时的语义与 `ParseTree` 结果单策略专属（禁共享）写入文档。

### Added（v1.8.0）
- `lockstep` 包：帧同步（输入帧）房间层，依赖 cube-core v1.7.0 的 `lockstep` 包（go.mod 已升级到正式版本）。`Room` 把 Sequencer/帧历史/冗余编码/裁决器绑定到传输：`Tick` 切帧并经 datagram 通道冗余广播（丢包靠后继报文冗余修复，不重传）；`StartCatchup` 重连追帧经可靠通道（KCP/QUIC 皆可，`ReliableSender` 接口互换）按 tick 限速分页，追上后自动切回实时；`ReportHash` 关键帧哈希多数派裁决，`OnDesync` 仅在裁决出现或离群集扩大时回调；掉线座位保留（缺席即空输入），重连 = `Attach` 换 session + 追帧。指标：`lockstep.frame.total`、`input.late.total`、`catchup.frames.total`、`desync.total`（非零即事故），见 cube-core OBSERVABILITY.md。

### Added（v1.7.1）
- `nestwal`：durability 管线指标——`nestwal.batch.total`/`append.total`（合批放大率）、`bytes.total`、`fsync.duration`、`pending.tickets`、`disk.bytes`、`reject.total{reason}`。面板与告警基线见 cube-core 的 OBSERVABILITY.md。

## [1.7.0] - 2026-08

### Added
- `spatial`：增量兴趣管理。`InterestManager`（九宫格订阅、双半径滞回、距离带 LOD、MaxVisible 风暴闸门、确定性 Flush）与 `InterestCluster`（共享坐标平面多房间无缝：边界 observer 镜像、净变化 Flush、跨界迁移 make-before-break 零闪断；单锁并发安全）。`BlockIndex` 新增 `QueryBlockIndex`。跨进程 handover 不在此层（见包注释）。
- `ai`：行为树二期。节点库（Parallel/Repeat/UntilSuccess/Succeeder/Condition/Guard/Cooldown/TimeLimit/RandomSelector，计时读注入 tick 时钟、随机用注入掷点）；`BehaviorStrategy`（树 → cube-core Strategy 桥，动作完成事件缓冲进下一 tick）；`TaskflowAction` 叶子（发起并等待 taskflow 动作，含打断收尾钩子）；`ParseTree`/`Registry`（严格 JSON 树装配，fail-fast + JSON path 诊断，schema `cube.ai/v1`）。

## [1.6.3] - 2026-08

### Fixed
- `redis/AutoExtendLock`：单次瞬时 Extend 错误不再永久停摆（TTL 预算内重试）；每次续期带独立超时，Redis 挂死不再拖死续期协程。
- `checkpoint`：`Stop` 在 WAL fence 场景下增加 30s 上界，不再无限悬挂。
- `nestwal`：`Stats()/Healthy()` 缓存最早未 ack 时间戳（5s TTL，Ack 时失效），周期健康检查不再全量 CRC 重扫段文件。

### Added
- CI 增加 `release-hygiene` 门禁（module 路径可解析 + tag 与 major 匹配）。

### Changed
- 锁双轨契约边界正式化：`redis.IDistLock` 包注释与 README 写明"无栅栏、仅限可容忍双执行的场景"，正确性互斥指向 `remoteentity` versionedLock / `etcd.IFencedElection`（含二选一判据表）。
- `spatial`/`ai`/`gateway` 各自的包注释补 non-goals 定位声明（无 Z 轴/navmesh/兴趣管理；行为树骨架而非 AI 中间件；中间件集合而非网关服务器）。

## [1.6.1] - 2026-08

- 对接 cube-core v1.6.x `DurabilityPipelined`：nestwal Enqueue ticket、durable watermark 先于唤醒发布、从 ack fence 起扫描重放、`NestOptions` 配置接线（`nest.pipelined.allowlist/async`）与 checkpoint 外化闸门（`SetDurableWatermark`）。
- `remoteentity` versionedLock 幂等 unlock；`etcd` 选主暴露 fence（`IFencedElection`）；`spatial` 寻路预算错误与半径查询饱和防护。
