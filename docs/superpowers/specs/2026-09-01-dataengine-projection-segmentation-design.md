# Data Engine 安全投影优化设计

**日期：** 2026-09-01  
**范围：** `roost-kit/dataengine` 的 WAL replay 投影调度、Mongo 批处理边界、正确性与性能门禁。

## 1. 背景

Data Engine 当前以 WAL 作为唯一持久化入口，由 Projector 顺序读取未确认记录并投影到
MongoDB。普通单文档记录可以通过 `ProjectBatch` 在一个 Mongo transaction 中执行有序
bulk write；包含 Saga receipt、effect、remote mutation、migration 或多文档 mutation 的
记录保留逐事务投影。

当前 Projector 先尝试把整个 replay batch 交给 `ProjectBatch`。只要 batch 中包含一条不支持
批量投影的记录，整个 batch 就降级为逐条 `Project`，最后才统一推进 WAL ack。这样既损失普通
记录的批处理收益，也存在部分投影成功、后续记录临时失败时从 batch 起点重放的风险。对于连续
修改同一实体的单文档 fast path，较早记录的 `_last_tx` 可能已被后续成功记录覆盖，重放时可能
被误判为永久版本冲突。

本轮优化首先修正执行与 ack 边界，再提高混合负载的批处理利用率。它不引入新的事务凭证格式、
并行 Projector、Patch 折叠或第二条持久化路径。

## 2. 目标

1. 将 replay batch 规划为保持原始 WAL 顺序的连续 projection segment。
2. 普通单文档记录连续段继续使用 Mongo `ProjectBatch`；特殊记录逐事务执行。
3. 每个 segment 投影成功后立即 ack 到该段最后一个 fence，ack 成功后才处理下一段。
4. 同时以记录数和保守逻辑字节数限制普通批量事务大小。
5. 保持现有事务标记、Saga、Receipt、Effect、Remote 和 Migration 语义不变。
6. 以单元测试、真实 Mongo/JetStream 集成测试、故障注入和压力基线证明变更。

## 3. 非目标

- 不新增批次事务凭证或修改 `_dataengine_transactions` 文档格式。
- 不删除每条普通批量记录对应的事务标记。
- 不并行执行多个 projection segment。
- 不合并或跳过中间 Patch。
- 不改变 Saga 的 Mongo transaction 边界。
- 不改变 Outbox worker 的并发、lease、重试或 MsgID 行为。
- 不改变 WAL 编码、durability、group commit 或 checkpoint 格式。
- 不恢复旧 Checkpoint 业务写路径。

## 4. 必须保持的不变量

### 4.1 持久化和确认

- WAL 是 Nest/Data Engine 事务的唯一 durable admission 路径。
- Mongo projection 成功之前不得推进对应 WAL fence。
- 一个 segment 的 WAL ack 失败后不得处理后续 segment。
- ack 失败导致的重放必须通过现有事务标记或单文档 `_last_tx` 保持幂等。
- Projector 只确认从当前 WAL ack fence 开始的连续成功前缀，不允许越过失败记录。

### 4.2 数据一致性

- 同一实体的 mutation 严格保持 WAL 顺序和 version CAS。
- 多文档 mutation、Receipt、Effect、Remote mutation 必须维持原有单事务原子性。
- 相同 transaction ID、不同 digest 必须继续触发永久 identity fence。
- Migration 的 obsolete CAS 仍按现有逻辑视为可确认 noop。
- NATS 故障不得阻止 Mongo projection 和 WAL ack；Effect 仍由 Outbox 异步投递。

### 4.3 兼容性

- Mongo collection、index 和 transaction marker schema 不变。
- 配置未设置新字节上限时必须使用安全默认值。
- 现有只实现 `ProjectionStore` 的自定义 store 继续逐条投影；planner 产生的多记录普通 segment
  在执行阶段展开为 singleton `Project` + ticket/stat + ack 单元，前一条 ack 成功后才执行下一条。
- 现有多条普通 replay batch 的一次 Mongo transaction + 一次 WAL ack 行为保持不变；单条普通
  segment 仍使用无 Mongo transaction 的单文档 fast path。

## 5. 设计

### 5.1 Projection segment

Projector 在一次 `replayPass` 中仍顺序读取最多 `ReplayBatchRecords` 条记录及其 fence，然后由
包内私有 planner 生成连续 segment：

```go
type projectionSegment struct {
    records []coredata.CommitRecord
    fences  []corenest.CommitFence
    batch   bool
}
```

planner 只进行确定性分类，不执行 I/O：

- 连续、可批量的普通记录组成一个 `batch=true` segment。
- 不可批量记录各自组成一个单记录 `batch=false` segment。
- 普通 segment 达到记录数或逻辑字节上限时切分。
- 输出顺序与输入完全相同，每条输入记录恰好出现一次。

可批量条件与当前 `MongoStore.ProjectBatch` 完全一致：

- 非 Migration；
- 恰好一个 mutation；
- 没有 Effect 和 Receipt；
- mutation 不是 Remote。

分类函数保留在 `dataengine` 包内并由 Projector 与 MongoStore 共用，避免两处规则漂移。

### 5.2 Segment 执行和 ack

Projector 顺序执行 planner 输出：

1. `batch=true`、记录数大于 1 且 store 支持 `BatchProjectionStore` 时调用 `ProjectBatch`。
2. store 不支持 `BatchProjectionStore` 时，多记录普通 segment 按输入 subslice 展开为 singleton
   执行单元；每个单元独立 `Project`、完成 ticket/统计并 ack，ack 失败立即停止。
3. 单条普通 segment 与特殊 segment 调用 `Project`，保留现有单文档 fast path；特殊 segment
   固定只有一条记录。
4. Mongo 成功后完成该执行单元中的 system projection ticket，并增加 Mongo projection 成功统计。
5. 调用 `WAL.Ack`，fence 为该执行单元最后一条记录的 fence。
6. ack 成功后更新 admitted/WALUnacked 统计，再继续下一执行单元。
7. Mongo 或 ack 失败时立即返回，不执行后续执行单元。

如果 Mongo 成功而 ack 失败，当前 segment 会被重放：

- 普通批量段由现有 `_dataengine_transactions` marker 验证并跳过已提交记录；
- 单条普通 fast path 的 `_last_tx` 仍指向当前事务，因为后续 segment 尚未执行；
- Saga/Receipt/Effect/Remote 路径由现有 transaction marker 验证。

这消除了“早期 fast-path 记录已成功、后续记录失败、但早期 `_last_tx` 又被覆盖”的窗口。

### 5.3 逻辑字节上限

`ProjectorOptions` 新增 `ReplayBatchBytes`，默认值为 4 MiB，并通过
`dataengine.projection.batch_bytes` 配置。它只限制普通 batch segment，不限制单条特殊记录。

逻辑大小采用无分配或低分配的保守估算，统计：

- CommitRecord 固定字段、Handler、RequestID；
- mutation key、codec、Data、SetBSON 和 Unset path；
- Effect topic/key/header/payload；
- Receipt namespace/id/digest/payload；
- Remote mutation 的序列化载荷按已有字段长度估算；
- 每个 slice、string 和 BSON 字段增加固定安全开销。

如果第一条可批量记录本身超过 `ReplayBatchBytes`，它单独形成一个 batch segment，避免 planner
无法前进。该限制是 Mongo transaction 的保护阈值，不声称精确等于 BSON wire size。

### 5.4 代码边界

- `projector.go`：WAL replay、segment 顺序执行、ticket、统计和 segment 级 ack。
- 新建 `projection_plan.go`：批量资格判断、逻辑大小估算和纯函数 planner。
- `mongo_store.go`：继续负责实际 Mongo batch transaction，不负责 WAL fence。
- `outbox_worker.go`、`saga/`、`nestwal/`：本轮不改变行为。

planner 使用包内私有类型，避免为了当前唯一 Mongo 后端扩张公共 API。现有
`BatchProjectionStore` 接口保持兼容。

## 6. 错误处理

- planner 发现记录/fence 长度不一致属于内部错误，Projector 返回错误且不 ack。
- `ProjectBatch` 返回 `errProjectionBatchUnsupported` 表示分类规则漂移，Projector 不得把已经包含
  多条记录的 segment 静默逐条执行；它返回错误以便测试和健康检查暴露问题。该错误必须在任何
  Mongo 写入之前返回。
- Mongo 临时错误按现有指数退避重试，重放从最后成功 ack 的 segment 之后开始。
- version、transaction identity、receipt identity conflict 继续触发 fatal fence。
- WAL ack 错误沿用 WAL 的 terminal/health 语义；Projector 不继续处理后续记录。
- context cancel 在当前 I/O 返回后结束，不创建脱离生命周期的投影 goroutine。

## 7. 测试策略

### 7.1 Planner 单元测试

- 全普通记录生成一个 segment。
- 普通/特殊/普通输入生成三个有序 segment。
- 记录数边界正确切分。
- 字节边界正确切分，超大单条仍可前进。
- 输入记录和 fence 不丢失、不重复、不重排。
- Migration、Effect、Receipt、Remote、多 mutation 均不可批量。

### 7.2 Projector 回归测试

- 混合 batch 的普通段使用 `ProjectBatch`，特殊记录使用 `Project`。
- 每个成功 segment 分别 ack；后续失败不会越过失败 fence。
- Mongo 成功、ack 失败后不执行后续 segment。
- 第一个 segment 成功、第二个失败，下一次 replay 不重新读取已 ack 的第一段。
- batch classifier 与 MongoStore 支持条件一致，规则漂移立即失败。
- system projection ticket 只在 Mongo 成功后完成；WALUnacked 只在 ack 成功后减少。
- 只实现 `ProjectionStore` 时，连续普通记录严格按 `Project(1) -> ack(1) -> Project(2) -> ack(2)`；
  第一条 ack 失败不执行第二条，第二条 Project 失败时第一条已经从 WAL 前缀移除。

### 7.3 真实基础设施测试

- 普通、Effect、普通记录混合，注入 Effect transaction 临时失败，恢复后版本无 gap。
- 同一实体在失败点前后连续修改，重启 Projector 后不产生错误 conflict。
- Mongo commit 成功后模拟 ack 前崩溃，重启后无重复 Receipt/Effect/Saga 推进。
- 100k 热点 backlog 保持最终 version、marker 数和无未确认 WAL。
- 新增多实体与混合比例压力矩阵，记录吞吐但不在开发机上设置不可靠的硬性能阈值。

### 7.4 最终故障恢复证据

- **真实 Mongo + 真实文件 WAL：** `TestRealProjectionOnlyMongoAckFailureRestartPreservesSameEntityOrder`
  用只暴露 `Project` 的 `MongoStore` wrapper 写同一实体的 version 1、2。第一条 Mongo 成功后注入 ack
  失败，断开并重新打开同一 WAL，再由新 Projector 重放。最终 version=2、值顺序正确、WAL 为空，
  `FatalProjectionConflicts=0`，证明不会因第二条提前覆盖 `_last_tx` 产生虚假 conflict。
- **真实文件 WAL + fake store（不含 Mongo）：**
  `TestProjectorTransientSpecialFailureRecoversFromAcknowledgedOrdinaryPrefix` 证明普通 batch 前缀已 ack，
  临时失败的特殊记录及后缀仍可重放；清除临时错误后只处理该后缀并清空 WAL。
- **真实文件 WAL + fake store（不含 Mongo）：**
  `TestProjectorBatchUnsupportedDoesNotFallbackOrRunLaterSegment` 证明
  `errProjectionBatchUnsupported` 不降级、不 ack，也不执行后续 segment。

### 7.5 全量门禁

- `go test ./...`。
- `go test -race` 覆盖 `dataengine`、`nestwal`、`saga`。
- `go vet` 覆盖 go.work 中四个模块。
- `scripts/integration/dataengine-env.sh test`。
- 优化前后重复运行 100k backlog 和 Saga 基线，报告原始数字与变化比例。

## 8. 性能预期与取舍

全普通 backlog 仍保持一次 Mongo batch transaction 和一次 segment ack，不因本设计增加额外 I/O。
混合负载不再因为一条特殊记录导致整个 replay batch 降级，普通连续段可保留 bulk write 收益。
特殊记录较多时 segment ack 次数会增加，但这是为了缩小部分提交重放窗口；Saga/Effect 本身已有
Mongo transaction 固定成本，额外 checkpoint 成本需要通过混合比例基线记录。

本轮不承诺解决 `_dataengine_transactions` 的长期存储放大。真实 `collStats` 和 profiler 数据若
证明 transaction marker 是主要瓶颈，再为 Transaction Ledger v2 单独设计可滚动升级、回滚兼容
和 crash recovery 协议，避免把未经证明的存储格式复杂度混入本次优化。

### 8.1 本机回归测量

以下数据来自同一台 Apple M5、darwin/arm64 开发机上的隔离环境
`/private/tmp/roost-dataengine-it`。它们只用于本机功能与回归比较，不代表生产容量，也不设置性能
通过阈值。

优化前基线沿用
`docs/superpowers/specs/2026-09-01-dataengine-local-integration-design.md` 的原始记录：100k backlog
append 446.917ms（223,755 records/s），projection 4.408s（22,688 records/s），WAL
21,360,637 bytes，98 个 ack batch；1,000 次 Saga receipt transaction 为 8.562s
（117 records/s）。

最终修复代码重新运行：

```text
100k backlog: append=562.900709ms (177651 records/s), projection=4.146900375s (24114 records/s), wal_bytes=21327039 ack_batches=98
Saga receipt transactions: 9.247436875s (108 records/s)
```

相对历史优化前单次基线，100k append 耗时增加 25.9520%，显示吞吐下降 20.6047%；projection
耗时下降 5.9233%，显示吞吐提高 6.2853%；WAL 减少 33,598 bytes（0.1573%），ack batch 保持
98。Saga 耗时增加 8.0056%，显示吞吐下降 7.6923%。相对 reviewed head `c99e06d` 的单次记录，
projection 耗时下降 2.4949%、吞吐提高 2.5560%，Saga 耗时下降 4.4676%；append 单次抖动较大，
耗时增加 18.7868%。这些开发机单样本受本机负载影响，只作为可复现参考，不是容量结论。

特殊 singleton 改为输入 subslice 后，Projection segment planner 的三次有界 `benchmem` 样本如下。
输入 `records/fences` 在整个同步 `replayPass` 中存活，segment 不越过该生命周期，因而 subslice 不改变
ownership；与 reviewed head 相比，1% special 的 allocs/op 从 28 降到 6、10% 从 215 降到 9、
all-special 从 2059 降到 11：

```text
command: GOCACHE=/private/tmp/roost-go-cache-final go test ./dataengine -run '^$' -bench '^BenchmarkProjectionSegmentPlanner$' -benchmem -benchtime=100x -count=3
goos: darwin
goarch: arm64
pkg: github.com/tjbdwanghaibo/cube-kit/dataengine
cpu: Apple M5
BenchmarkProjectionSegmentPlanner/special_every_0-10         	     100	     27516 ns/op	      64 B/op	       1 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_0-10         	     100	     24296 ns/op	      64 B/op	       1 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_0-10         	     100	     21366 ns/op	      64 B/op	       1 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_100-10       	     100	     20030 ns/op	    3920 B/op	       6 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_100-10       	     100	     20418 ns/op	    3920 B/op	       6 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_100-10       	     100	     17365 ns/op	    3920 B/op	       6 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_10-10        	     100	     22595 ns/op	   32592 B/op	       9 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_10-10        	     100	     18806 ns/op	   32592 B/op	       9 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_10-10        	     100	     18981 ns/op	   32592 B/op	       9 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_1-10         	     100	     17123 ns/op	  122762 B/op	      11 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_1-10         	     100	     18326 ns/op	  122705 B/op	      11 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_1-10         	     100	     18630 ns/op	  122705 B/op	      11 allocs/op
PASS
ok  	github.com/tjbdwanghaibo/cube-kit/dataengine	0.343s
```

### 8.2 文件 WAL replay/ack 有界工作负载（不含 Mongo）

该 benchmark 使用真实 `nestwal` 文件、真实 replay/checkpoint ack，以及内存
`BatchProjectionStore`；store 不执行 Mongo I/O，因此这些数字**不是 Mongo 吞吐**。每个 workload
含 256 条记录、32 个 entity ID；命令用 `-benchtime=5x` 显式限制运行次数：

```text
command: GOCACHE=/private/tmp/roost-go-cache-final go test ./dataengine -run '^$' -bench '^BenchmarkProjectorWALReplayAckMatrix$' -benchmem -benchtime=5x -count=1
BenchmarkProjectorWALReplayAckMatrix/ordinary_only-10         	       5	  10646517 ns/op	         1.000 acks/op	         1.000 batch_calls/op	         0 project_calls/op	  325910 B/op	    9204 allocs/op
BenchmarkProjectorWALReplayAckMatrix/special_1_percent-10     	       5	  44892583 ns/op	         5.000 acks/op	         3.000 batch_calls/op	         2.000 project_calls/op	  333796 B/op	    9285 allocs/op
BenchmarkProjectorWALReplayAckMatrix/special_10_percent-10    	       5	 437464683 ns/op	        51.00 acks/op	        26.00 batch_calls/op	        25.00 project_calls/op	  411782 B/op	   10171 allocs/op
BenchmarkProjectorWALReplayAckMatrix/all_special-10           	       5	2250236659 ns/op	       256.0 acks/op	         0 batch_calls/op	       256.0 project_calls/op	  773654 B/op	   15445 allocs/op
PASS
ok  	github.com/tjbdwanghaibo/cube-kit/dataengine	42.524s
```

ack 次数与 segment 形状一致，并且每次 iteration 在计时后重新 `WAL.Replay` 验证剩余记录为 0；
all-special 的 checkpoint 固定成本是这个 WAL-only/fake-store benchmark 的主要耗时。

### 8.3 真实 Mongo + 文件 WAL 多实体混合比例

`TestRealMongoMixedRatioWALReplayAckThroughput` 使用真实三节点 Mongo replica set、真实文件 WAL 和
真实 `MongoStore`，同样覆盖 256 条记录、16 个 entity ID；特殊记录使用 receipt，测试不设置容量
阈值。以下数字只属于这台本机隔离环境，且与 8.2 的 fake-store 数字不可混用：

```text
backend=real MongoDB + file WAL workload=ordinary_only records=256 entities=16 special=0 checkpoint_acks=1 elapsed=66.53875ms throughput=3847 records/s
backend=real MongoDB + file WAL workload=special_1_percent records=256 entities=16 special=2 checkpoint_acks=5 elapsed=166.984417ms throughput=1533 records/s
backend=real MongoDB + file WAL workload=special_10_percent records=256 entities=16 special=25 checkpoint_acks=51 elapsed=1.312427s throughput=195 records/s
backend=real MongoDB + file WAL workload=all_special records=256 entities=16 special=256 checkpoint_acks=256 elapsed=6.176983709s throughput=41 records/s
```

每个 shape 均验证 16 个实体的最终 version=16、receipt 数、`Projected=256` 和 WAL 为空。

## 9. 验收标准

- 持久化格式、Saga 原子性、Outbox 语义和 WAL durability 均未改变。
- 混合 batch 按连续 segment 投影；无 batch 能力的多记录普通 segment 展开为 singleton 执行单元，
  两种路径都逐执行单元 ack。
- 任一执行单元失败后，后续执行单元没有 Mongo 写入且 WAL ack 不越过失败点。
- 普通单文档 fast path 在部分失败和重放场景下不会因 `_last_tx` 被覆盖而产生虚假 fatal conflict。
- 记录数与逻辑字节双上限均有单元测试。
- 单元、race、vet、真实 Mongo/JetStream 集成和故障测试全部通过。
- 性能报告明确区分热点 backlog、多实体、Saga transaction 和故障恢复耗时。
