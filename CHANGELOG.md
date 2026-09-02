# Changelog

本文件从 v1.6.1 起维护；更早版本见 git 历史。

## [Unreleased]

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
- 锁双轨契约边界正式化：`redis.IDistLock` 包注释与 README 写明"无栅栏、仅限可容忍双执行的场景"，正确性互斥指向 `remote_entity` versionedLock / `etcd.IFencedElection`（含二选一判据表）。
- `spatial`/`ai`/`gateway` 各自的包注释补 non-goals 定位声明（无 Z 轴/navmesh/兴趣管理；行为树骨架而非 AI 中间件；中间件集合而非网关服务器）。

## [1.6.1] - 2026-08

- 对接 cube-core v1.6.x `DurabilityPipelined`：nestwal Enqueue ticket、durable watermark 先于唤醒发布、从 ack fence 起扫描重放、`NestOptions` 配置接线（`nest.pipelined.allowlist/async`）与 checkpoint 外化闸门（`SetDurableWatermark`）。
- `remote_entity` versionedLock 幂等 unlock；`etcd` 选主暴露 fence（`IFencedElection`）；`spatial` 寻路预算错误与半径查询饱和防护。
