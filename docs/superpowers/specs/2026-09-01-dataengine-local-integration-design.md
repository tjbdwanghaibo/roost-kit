# Data Engine 本机隔离集成环境设计

**日期：** 2026-09-01
**范围：** `roost-kit` 真实基础设施测试、隔离环境开关脚本，以及通过门禁后的旧 Checkpoint 删除。

## 1. 目标

在不修改、不停止当前 Homebrew MongoDB、NATS、etcd、Redis 服务的前提下，使用本机已有
`mongod`、`mongosh` 和 `nats-server` 二进制建立可重复启停的隔离测试环境，完成 Data
Engine 真实事务、持久消息、故障切换和积压恢复测试。测试通过后，删除已经不再承担 Data
Engine 职责的旧 Checkpoint 写运行时。

本设计不把本机测试结果描述为生产容量证明；它证明功能语义和故障恢复路径可运行。生产容量
仍需在目标机器、磁盘和网络拓扑上单独测量。

## 2. 当前环境与缺口

- MongoDB 8.3.7 正在 `127.0.0.1:27017` 运行，但为 standalone，没有 replica set，
  无法执行 Data Engine 多文档事务和 primary failover。
- NATS 2.14.x 正在 `127.0.0.1:4222` 运行，但未启用 JetStream，无法验证 file
  storage、stream leader failover、redelivery 和 MsgID 去重。
- etcd 3.7.1 单节点健康。Data Engine 的 WAL、Mongo projection 和 effect outbox 不依赖
  etcd，因此本轮不另建 etcd 集群；框架级 etcd compaction/failover 仍属于独立门禁。
- Redis 正常。新 Data Engine 不依赖 Redis；它仅用于旧 Checkpoint 清理前的兼容检查。

## 3. 隔离拓扑

所有监听地址均为 `127.0.0.1`，数据、日志、配置和 PID 文件统一位于：

```text
/private/tmp/roost-dataengine-it/
```

### MongoDB

建立 replica set `roost-it`：

| 节点 | Client port | dbPath |
|---|---:|---|
| mongo-1 | 27117 | `mongo-1/data` |
| mongo-2 | 27118 | `mongo-2/data` |
| mongo-3 | 27119 | `mongo-3/data` |

使用 majority write concern、journaling 和标准 election。测试 URI 为：

```text
mongodb://127.0.0.1:27117,127.0.0.1:27118,127.0.0.1:27119/?replicaSet=roost-it
```

### NATS JetStream

建立三节点集群 `roost-it`，每个节点启用独立 file store：

| 节点 | Client | Route | Monitor |
|---|---:|---:|---:|
| nats-1 | 14222 | 16222 | 18222 |
| nats-2 | 14223 | 16223 | 18223 |
| nats-3 | 14224 | 16224 | 18224 |

Data Engine 测试 stream 使用 `replicas=3`。客户端连接串为：

```text
nats://127.0.0.1:14222,nats://127.0.0.1:14223,nats://127.0.0.1:14224
```

## 4. 环境脚本

新增 `roost-kit/scripts/integration/dataengine-env.sh`，提供：

```text
up       启动缺失节点、初始化 Mongo replica set、等待 Mongo primary 和 NATS cluster ready
down     只停止 PID 文件指向且命令行属于本环境的进程，保留数据
status   输出每个节点、Mongo replica set、JetStream cluster/stream 的健康状态
reset    先安全 down，再删除经过固定根目录校验的数据目录
test     up 后运行完整 integration suite，失败时保留日志和数据，成功后默认保留环境
```

脚本必须满足：

1. 重复执行 `up`、`down` 和 `status` 是幂等的。
2. 启动前检查二进制与端口冲突，错误信息给出缺失命令或占用端口。
3. 不使用宽泛 `pkill`，不停止 27017/4222/2379/6379 上的 Homebrew 服务。
4. `reset` 只允许删除精确的 `/private/tmp/roost-dataengine-it`，目录不匹配时拒绝执行。
5. 启动和 readiness 使用有截止时间的条件轮询，不使用固定长时间 sleep。
6. 生成 `env.sh`，包含测试所需 URI、database、stream、subject 和 WAL 目录变量。

## 5. 集成测试

真实基础设施测试使用 `//go:build integration`，普通 `go test ./...` 不自动启动服务。脚本
`test` 负责设置环境变量并执行：

```text
go test -tags=integration ./dataengine ./nestwal ./saga
```

测试至少覆盖：

1. **事务原子性**：多文档 mutation、transaction receipt 和 effect outbox 在同一 Mongo
   transaction 内成功；注入失败后均不可见。
2. **版本 fencing**：Patch 只接受 `ExpectedVersion`，冲突不会 full fallback 或静默覆盖。
3. **真实 load/migrate**：从 projection load，migration 经 system transaction 写入并恢复
   tracker version。
4. **Effect 解耦**：停止 NATS 后 Mongo projection 和 WAL ACK 继续；恢复后 backlog 发布，
   稳定 EffectID 不造成重复业务执行。
5. **Mongo primary failover**：识别并停止当前 primary，等待新 primary，在驱动重试边界内继续
   projection；重启旧节点后恢复三节点健康。
6. **JetStream leader failover**：创建三副本 stream，停止当前 stream leader，等待新 leader，
   验证 publish、redelivery 和 durable consumer 继续工作。
7. **10 万 WAL 积压恢复**：生成 100,000 条 versioned record，重启 projector 后验证无 version
   gap、无重复 receipt、无重复 Saga 推进，并记录恢复吞吐和耗时。

故障测试必须通过脚本提供的精确节点控制命令执行；测试结束后应恢复被停止节点。测试失败时
打印日志路径、replica set 状态和 JetStream 状态。

## 6. Checkpoint 删除顺序

Checkpoint 删除以真实集成测试全部通过为前置条件，按以下顺序执行：

1. 增加 architecture guard，禁止生产生成代码引用 `checkpoint.SaveItem`、`Snapshot`、
   `RemoveSnapshot`、Checkpoint journal 或 Redis SnapshotWAL。
2. 删除 core 的 Checkpoint 写入模型、Journal、Flusher、Redis SnapshotWAL 和 release hook。
3. 删除 kit 的 Checkpoint mod、Redis WAL 配置和旧运行时装配。
4. 删除 codegen 的 legacy Checkpoint 项目选项、import、生命周期错误类型和兼容模板。
5. 将迁移文档改为“升级前备份/历史说明”，不保留可再次启用的第二写路径。
6. 执行 core、kit、skill、codegen 全量测试、race、vet、真实 integration suite 和引用扫描。

删除后只允许保留与历史数据读取确实相关、且不能形成写路径的短期迁移代码。研发期不再等待
生产 patch-only 观察窗口，但真实 Mongo/JetStream 功能门禁不得跳过。

## 7. 安全与失败处理

- 环境脚本不需要管理员权限，不修改 `/opt/homebrew/etc` 和 Homebrew service plist。
- 每个进程使用独立 PID 文件；停止前同时验证 PID 存活、可执行文件名和环境根目录参数。
- 端口被非本环境进程占用时直接失败，不尝试杀死占用者。
- Mongo/NATS 启动或选主超时后停止本次新启动的节点，但保留日志。
- 集成测试使用独立 database、stream、consumer 和 WAL 目录，名称包含固定 `roost_it`
  前缀；清理由精确名称完成。

## 8. 验收标准

- `dataengine-env.sh up|down|status|reset|test` 均可重复执行并给出明确结果。
- 当前 Homebrew 服务的 PID 和端口在测试前后不变。
- Mongo 三节点 replica set 和 NATS 三节点 JetStream file storage 状态可由脚本证明。
- 第 5 节全部真实集成测试通过，普通单元测试仍通过。
- 旧 Checkpoint 写路径和生成入口已物理删除，引用扫描只有历史文档允许项。
- 四个 Go workspace 模块的全量测试、相关 race 和 vet 全部通过。
