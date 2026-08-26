# Changelog

本文件从 v1.6.1 起维护；更早版本见 git 历史。

## [Unreleased]

### Fixed（v1.7.0 发布后独立复审的 8 项发现，均带回归测试）
- `spatial`：MaxVisible 从每房间独立执行收口为 **cluster 级全局 top-N**（缝边 observer 的预算曾被相交房间数放大）；`AddRoom` 回填既有 observer 的边界镜像（静止 observer 对后加房间曾有永久可见性盲区）；拒绝 id 0（曾半跟踪导致可见性对不同 observer 分裂）；`LeaveRadius` 加上界 + 订阅盒算术全饱和（极值曾回绕致 observer 全盲）；observer 全量评估改最近优先 admission（曾产生瞬态 Enter+Leave 对）；补 gofmt。
- `ai`：guard 谓词在装配期限定为无状态形态（condition/sequence/selector/inverter，action 作谓词曾每次检查都发起动作）；nil-root 策略丢弃动作完成事件（曾无界堆积）；`Cooldown.Reset` 保留计时的语义与 `ParseTree` 结果单策略专属（禁共享）写入文档。

### Added
- `nestwal`：durability 管线指标——`nestwal.batch.total`/`append.total`（合批放大率）、`bytes.total`、`fsync.duration`、`pending.tickets`、`disk.bytes`、`reject.total{reason}`。面板与告警基线见 cube-core 的 OBSERVABILITY.md。
- `spatial`：增量兴趣管理。`InterestManager`（九宫格订阅、双半径滞回、距离带 LOD、MaxVisible 风暴闸门、确定性 Flush）与 `InterestCluster`（共享坐标平面多房间无缝：边界 observer 镜像、净变化 Flush、跨界迁移 make-before-break 零闪断；单锁并发安全）。`BlockIndex` 新增 `QueryBlockIndex`。跨进程 handover 不在此层（见包注释）。
- `ai`：行为树二期。节点库（Parallel/Repeat/UntilSuccess/Succeeder/Condition/Guard/Cooldown/TimeLimit/RandomSelector，计时读注入 tick 时钟、随机用注入掷点）；`BehaviorStrategy`（树 → cube-core Strategy 桥，动作完成事件缓冲进下一 tick）；`TaskflowAction` 叶子（发起并等待 taskflow 动作，含打断收尾钩子）；`ParseTree`/`Registry`（严格 JSON 树装配，fail-fast + JSON path 诊断，schema `cube.ai/v1`）。

## [Unreleased]（已提交，待发 v1.6.2）

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
