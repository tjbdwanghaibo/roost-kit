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
- 现有只实现 `ProjectionStore` 的自定义 store 继续逐条投影。
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
2. 单条普通 segment 与特殊 segment 调用 `Project`，保留现有单文档 fast path；特殊 segment
   固定只有一条记录。
3. Mongo 成功后完成该 segment 中的 system projection ticket，并增加 Mongo projection 成功统计。
4. 调用 `WAL.Ack`，fence 为该 segment 最后一条记录的 fence。
5. ack 成功后更新 admitted/WALUnacked 统计，再继续下一 segment。
6. Mongo 或 ack 失败时立即返回，不执行后续 segment。

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

### 7.3 真实基础设施测试

- 普通、Effect、普通记录混合，注入 Effect transaction 临时失败，恢复后版本无 gap。
- 同一实体在失败点前后连续修改，重启 Projector 后不产生错误 conflict。
- Mongo commit 成功后模拟 ack 前崩溃，重启后无重复 Receipt/Effect/Saga 推进。
- 100k 热点 backlog 保持最终 version、marker 数和无未确认 WAL。
- 新增多实体与混合比例压力矩阵，记录吞吐但不在开发机上设置不可靠的硬性能阈值。

### 7.4 全量门禁

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

优化后以本设计的最终代码重新运行：

```text
100k backlog: append=473.874791ms (211026 records/s), projection=4.253007167s (23513 records/s), wal_bytes=21372621 ack_batches=98
Saga receipt transactions: 9.679899667s (103 records/s)
```

按以上日志显示值计算，100k append 耗时增加 6.0319%，append 吞吐下降 5.6888%；projection
耗时下降 3.5162%，projection 吞吐提高 3.6363%；WAL 增加 11,984 bytes（0.0561%），ack batch
保持 98。Saga 耗时增加 13.0565%，显示吞吐下降 11.9658%。这些单次开发机结果受本机负载影响，
只能作为本次变更的可复现参考。

Projection segment planner 的三次 `benchmem` 样本如下：

```text
goos: darwin
goarch: arm64
pkg: github.com/tjbdwanghaibo/cube-kit/dataengine
cpu: Apple M5
BenchmarkProjectionSegmentPlanner/special_every_0-10         	   96606	     11003 ns/op	      64 B/op	       1 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_0-10         	  106351	     11168 ns/op	      64 B/op	       1 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_0-10         	  104752	     11090 ns/op	      64 B/op	       1 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_100-10       	   92749	     12382 ns/op	    5856 B/op	      28 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_100-10       	   98698	     12480 ns/op	    5856 B/op	      28 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_100-10       	   97557	     12789 ns/op	    5856 B/op	      28 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_10-10        	   66260	     18725 ns/op	   50720 B/op	     215 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_10-10        	   67575	     18039 ns/op	   50720 B/op	     215 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_10-10        	   63913	     19679 ns/op	   50720 B/op	     215 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_1-10         	   20744	     50001 ns/op	  302929 B/op	    2059 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_1-10         	   24508	     51490 ns/op	  302929 B/op	    2059 allocs/op
BenchmarkProjectionSegmentPlanner/special_every_1-10         	   24112	     51486 ns/op	  302929 B/op	    2059 allocs/op
PASS
ok  	github.com/tjbdwanghaibo/cube-kit/dataengine	17.690s
```

## 9. 验收标准

- 持久化格式、Saga 原子性、Outbox 语义和 WAL durability 均未改变。
- 混合 batch 按连续 segment 投影并逐 segment ack。
- 任一 segment 失败后，后续 segment 没有 Mongo 写入且 WAL ack 不越过失败点。
- 普通单文档 fast path 在部分失败和重放场景下不会因 `_last_tx` 被覆盖而产生虚假 fatal conflict。
- 记录数与逻辑字节双上限均有单元测试。
- 单元、race、vet、真实 Mongo/JetStream 集成和故障测试全部通过。
- 性能报告明确区分热点 backlog、多实体、Saga transaction 和故障恢复耗时。
