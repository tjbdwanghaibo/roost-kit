# Changelog

本文件从 v1.6.1 起维护；更早版本见 git 历史。

## [Unreleased]（已提交，待发 v1.6.2）

### Fixed
- `redis/AutoExtendLock`：单次瞬时 Extend 错误不再永久停摆（TTL 预算内重试）；每次续期带独立超时，Redis 挂死不再拖死续期协程。
- `checkpoint`：`Stop` 在 WAL fence 场景下增加 30s 上界，不再无限悬挂。
- `nestwal`：`Stats()/Healthy()` 缓存最早未 ack 时间戳（5s TTL，Ack 时失效），周期健康检查不再全量 CRC 重扫段文件。

### Added
- CI 增加 `release-hygiene` 门禁（module 路径可解析 + tag 与 major 匹配）。

## [1.6.1] - 2026-08

- 对接 cube-core v1.6.x `DurabilityPipelined`：nestwal Enqueue ticket、durable watermark 先于唤醒发布、从 ack fence 起扫描重放、`NestOptions` 配置接线（`nest.pipelined.allowlist/async`）与 checkpoint 外化闸门（`SetDurableWatermark`）。
- `remote_entity` versionedLock 幂等 unlock；`etcd` 选主暴露 fence（`IFencedElection`）；`spatial` 寻路预算错误与半径查询饱和防护。
