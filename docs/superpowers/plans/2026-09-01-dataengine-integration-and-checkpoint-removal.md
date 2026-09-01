# Data Engine Integration and Checkpoint Removal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** 建立可一键启停的本机 Mongo/NATS 三节点隔离环境，用真实事务和故障测试证明 Data Engine，然后物理删除旧 Checkpoint 写路径。

**Architecture:** `scripts/integration/dataengine-env.sh` 是唯一用户入口，内部 shell library 管理固定 `/private/tmp/roost-dataengine-it` 下的 Mongo replica set 和 NATS JetStream cluster。带 `integration` build tag 的 Go 测试通过 Roost 的 Mongo/NATS/Data Engine mod 连接真实节点；这些门禁全绿后，将 dirty/scope 契约迁入 `core/dataengine` 并移除 core/kit/codegen 的 Checkpoint 和 legacy NestWAL runtime。

**Tech Stack:** Bash、MongoDB 8.x replica set、NATS 2.x JetStream file storage、Go 1.26.5+、mongo-driver v2、nats.go、Roost Data Engine/Nest/Saga。

**Spec:** `docs/superpowers/specs/2026-09-01-dataengine-local-integration-design.md`

## Global Constraints

- 不修改或停止 Homebrew 的 27017/4222/2379/6379 服务。
- 环境根目录固定为 `/private/tmp/roost-dataengine-it`；`reset` 对其他路径必须拒绝。
- Mongo 使用 27117-27119、replica set `roost-it`；NATS 使用 client 14222-14224、route 16222-16224、monitor 18222-18224。
- 进程停止必须同时验证 PID、可执行文件和本环境参数；禁止宽泛 `pkill`。
- 普通 `go test ./...` 不启动或破坏外部服务；真实测试必须带 `integration` build tag 和 `ROOST_DATAENGINE_IT=1`。
- Checkpoint 删除只能在真实事务、Mongo primary failover、JetStream leader failover、NATS outage 和 100k backlog 门禁全部通过后开始。
- 每个仓库独立提交，现有无关改动不得覆盖。

---

### Task 1: 可重复启停的隔离基础设施脚本

**Files:**
- Create: `scripts/integration/dataengine-env.sh`
- Create: `scripts/integration/lib/common.sh`
- Create: `scripts/integration/lib/mongo.sh`
- Create: `scripts/integration/lib/nats.sh`
- Create: `scripts/integration/dataengine_env_test.sh`

**Interfaces:**
- Produces: `dataengine-env.sh up|down|status|reset|test|fault|heal`
- Produces: `/private/tmp/roost-dataengine-it/env.sh` exporting `ROOST_DATAENGINE_IT_MONGO_URI`, `ROOST_DATAENGINE_IT_NATS_URL`, `ROOST_DATAENGINE_IT_ROOT`
- Consumes: `mongod`, `mongosh`, `nats-server`, `curl`, `jq`, `nc`

- [x] **Step 1: 写缺失脚本和安全根目录的失败测试**

```bash
#!/usr/bin/env bash
set -euo pipefail
root="$(cd "$(dirname "$0")/../.." && pwd)"
script="$root/scripts/integration/dataengine-env.sh"
test -x "$script"
"$script" status >/dev/null || test "$?" -eq 1
ROOST_DATAENGINE_IT_ROOT=/tmp/not-roost "$script" reset 2>&1 | grep -q 'refuse unsafe root'
```

- [x] **Step 2: 运行 shell test 并确认失败**

Run: `bash scripts/integration/dataengine_env_test.sh`

Expected: FAIL because `dataengine-env.sh` does not exist.

- [x] **Step 3: 实现公共安全和 readiness helpers**

`lib/common.sh` 定义并由其他文件复用：

```bash
readonly ROOST_IT_ROOT="/private/tmp/roost-dataengine-it"
require_safe_root() {
  [[ "${ROOST_DATAENGINE_IT_ROOT:-$ROOST_IT_ROOT}" == "$ROOST_IT_ROOT" ]] || {
    echo "refuse unsafe root: ${ROOST_DATAENGINE_IT_ROOT:-}" >&2
    return 2
  }
}
wait_until() { # timeout seconds, description, command...
  local timeout="$1" description="$2"; shift 2
  local deadline=$((SECONDS + timeout))
  until "$@"; do
    (( SECONDS < deadline )) || { echo "timeout waiting for $description" >&2; return 1; }
    sleep 0.2
  done
}
```

增加 `require_commands`、`port_is_free_or_owned`、`read_owned_pid`、`stop_owned_pid`；`stop_owned_pid` 读取 PID 后用 `ps -p "$pid" -o command=` 验证命令包含 `$ROOST_IT_ROOT`，否则拒绝发送信号。

- [x] **Step 4: 实现 Mongo 三节点生命周期**

`lib/mongo.sh` 使用明确参数启动每个节点：

```bash
mongod --replSet roost-it --port "$port" --bind_ip 127.0.0.1 \
  --dbpath "$node/data" --logpath "$node/mongod.log" \
  --pidfilepath "$node/mongod.pid" --oplogSize 128 --fork
```

首次启动执行 `rs.initiate({_id:'roost-it',members:[...]})`，随后轮询 `db.adminCommand({hello:1}).isWritablePrimary`。实现 `mongo_status`、`mongo_primary_node`、`mongo_fault_primary` 和 `mongo_heal`，heal 重启缺失节点并等待三成员为 PRIMARY/SECONDARY。

- [x] **Step 5: 实现 NATS 三节点生命周期**

`lib/nats.sh` 为每个节点生成独立 config，包含：

```text
server_name: nats-1
listen: 127.0.0.1:14222
http: 127.0.0.1:18222
jetstream { store_dir: "/private/tmp/roost-dataengine-it/nats-1/store" }
cluster { name: roost-it; listen: 127.0.0.1:16222; routes: [...] }
```

使用 `nats-server -c "$config" -P "$pid"` 启动，轮询 `/varz` 的 `jetstream=true` 和 `/routez` 的 `num_routes>=2`。实现 `nats_status`、从 `/jsz?streams=true` 解析 stream leader 的 `nats_stream_leader_node`、`nats_fault_leader` 和 `nats_heal`。

- [x] **Step 6: 实现公共 dispatcher 和环境文件**

`dataengine-env.sh`：

```bash
case "${1:-}" in
  up) preflight; mongo_up; nats_up; write_env; status ;;
  down) nats_down; mongo_down ;;
  status) mongo_status && nats_status ;;
  reset) require_safe_root; "$0" down; rm -rf -- "$ROOST_IT_ROOT" ;;
  fault) shift; fault_dispatch "$@" ;;
  heal) mongo_heal; nats_heal ;;
  test) "$0" up; source "$ROOST_IT_ROOT/env.sh"; ROOST_DATAENGINE_IT=1 go test -tags=integration ./dataengine ./nestwal ./saga ;;
  *) usage; exit 2 ;;
esac
```

实际删除前再次比较根目录字面值；不得对变量未解析、空字符串或父目录执行 `rm -rf`。

- [x] **Step 7: 运行脚本测试和真实 up/status/down/up**

Run:

```bash
bash scripts/integration/dataengine_env_test.sh
scripts/integration/dataengine-env.sh up
scripts/integration/dataengine-env.sh status
scripts/integration/dataengine-env.sh down
scripts/integration/dataengine-env.sh up
```

Expected: shell test PASS；status 显示 Mongo 1 PRIMARY + 2 SECONDARY、NATS 3 nodes + JetStream；Homebrew 服务 PID 不变。

- [x] **Step 8: 提交环境脚本**

```bash
git add scripts/integration
git commit -m "test(dataengine): add isolated integration cluster"
```

### Task 2: 真实 Data Engine fixture 与事务语义

**Files:**
- Create: `dataengine/real_integration_test.go`
- Modify: `mongo/mongo_mod.go`
- Test: `dataengine/real_integration_test.go`

**Interfaces:**
- Produces: `newRealFixture(t *testing.T) *realFixture`
- Produces: `realFixture.close()` and unique Mongo database, stream, subject, WAL directory per test
- Consumes: Task 1 environment variables and `mongo.NewMongoMod`, `nats.NewNatsMod`, `dataengine.NewMod`

- [x] **Step 1: 写真实启动与原子事务失败测试**

测试文件带：

```go
//go:build integration

func requireIntegration(t *testing.T) {
    if os.Getenv("ROOST_DATAENGINE_IT") != "1" { t.Skip("set ROOST_DATAENGINE_IT=1") }
}

func TestRealMultiDocumentReceiptAndOutboxAreAtomic(t *testing.T) {
    fx := newRealFixture(t)
    defer fx.close()
    record := realCommitRecord(twoPutMutations(), oneReceipt(), oneEffect())
    if err := fx.runtime.Projector.Commit(context.Background(), record); err != nil { t.Fatal(err) }
    if err := fx.runtime.Flush(context.Background()); err != nil { t.Fatal(err) }
    assertProjectedVersions(t, fx.mongo, fx.database, 1, 1)
    assertReceiptAndOutbox(t, fx.mongo, fx.database, record)
}
```

另写 `TestRealPatchConflictFencesWithoutFullFallback`，先写 version=2，再提交 expected=1 patch，断言 `errors.Is(err, ErrProjectionConflict)` 且文档数据未变化。

- [x] **Step 2: 运行并确认因 fixture 不存在而失败**

Run: `ROOST_DATAENGINE_IT=1 go test -tags=integration ./dataengine -run 'TestReal(Multi|Patch)' -count=1 -v`

Expected: compile FAIL with undefined `newRealFixture`.

- [x] **Step 3: 实现 fixture，并仅补必要的 Mod 可测试入口**

fixture 创建 `viper.Viper`，设置：

```go
cfg.Set("persistence.engine", "dataengine")
cfg.Set("mongo.uri", os.Getenv("ROOST_DATAENGINE_IT_MONGO_URI"))
cfg.Set("mongo.require_replica_set", true)
cfg.Set("nats.url", os.Getenv("ROOST_DATAENGINE_IT_NATS_URL"))
cfg.Set("dataengine.database", database)
cfg.Set("dataengine.effects.replicas", 3)
cfg.Set("dataengine.wal.writer_version", 2)
cfg.Set("dataengine.wal.dir", filepath.Join(os.Getenv("ROOST_DATAENGINE_IT_ROOT"), "tests", t.Name(), "wal"))
```

按 Mongo Mod → NATS Mod → Data Engine Mod 顺序执行 Init/Provide/Start，反序 Stop；通过 Registry 获取真实 `fmongo.IMongo` 和 `fnats.IJetStream`。若清理需要原生 client，只给 `MongoMod` 增加窄接口 `DropDatabase(context.Context,string) error`，不暴露 mongo-driver client。

- [x] **Step 4: 实现真实 transaction、patch 和 load/migrate 断言**

使用 BSON 查询 `_version`、`_last_tx`、receipt/outbox collection；补 `TestRealLoadAndMigrationRestoresTrackerVersion`，断言 migration ticket projection 完成后才发布 entity，tracker version 与 Mongo 一致。

- [x] **Step 5: 运行真实测试和普通测试**

Run:

```bash
ROOST_DATAENGINE_IT=1 go test -tags=integration ./dataengine -run 'TestReal' -count=1 -v
go test ./dataengine ./mongo ./nats -count=1
```

Expected: PASS.

- [x] **Step 6: 提交真实事务测试**

```bash
git add dataengine/real_integration_test.go mongo/mongo_mod.go
git commit -m "test(dataengine): verify real transactional projection"
```

### Task 3: 故障切换、Outbox 和 100k backlog 门禁

**Files:**
- Create: `dataengine/failover_integration_test.go`
- Create: `nestwal/backlog_integration_test.go`
- Modify: `scripts/integration/dataengine-env.sh`
- Modify: `scripts/integration/lib/mongo.sh`
- Modify: `scripts/integration/lib/nats.sh`

**Interfaces:**
- Consumes: Task 1 `fault mongo-primary`, `fault nats-leader <stream>`, `heal`
- Produces: integration tests that always register `t.Cleanup(healEnvironment)`

- [x] **Step 1: 写 NATS outage 和 Mongo primary failover 失败测试**

```go
func TestRealNATSOutageDoesNotBlockProjectionAndRecoversOutbox(t *testing.T) {
    fx := newRealFixture(t)
    stopAllNATS(t); t.Cleanup(healEnvironment)
    commitAndFlush(t, fx, recordWithEffect("effect-outage"))
    assertProjectionVisible(t, fx)
    assertOutboxPending(t, fx, 1)
    healEnvironment(t)
    require.Eventually(t, func() bool { return outboxPending(fx) == 0 }, 30*time.Second, 100*time.Millisecond)
    assertEffectHandledOnce(t, fx, "effect-outage")
}
```

Mongo 用例停止脚本识别出的 primary，等待新 primary 后提交新版本，最后 heal 并验证三成员健康。

- [x] **Step 2: 运行并确认 fault helpers 不存在而失败**

Run: `ROOST_DATAENGINE_IT=1 go test -tags=integration ./dataengine -run 'TestReal(NATS|MongoPrimary)' -count=1 -v`

Expected: compile FAIL with undefined fault helpers.

- [x] **Step 3: 实现精确 fault/heal 控制和 Go helper**

Go helper 只执行仓库内 `dataengine-env.sh fault ...`；shell 根据 PID ownership 校验停止一个节点，输出节点名供测试记录。所有失败路径通过 `t.Cleanup` 调用 heal。

- [x] **Step 4: 写 JetStream leader failover 测试**

创建 replicas=3 stream，发布一条消息，停止 `/jsz` 报告的 leader，等待 leader 变化，再发布相同 MsgID 和新 MsgID；断言 duplicate ack 和 durable consumer 顺序正确。

- [x] **Step 5: 写 100k WAL → Mongo backlog 恢复测试**

`nestwal/backlog_integration_test.go` 先停止 projector，向 v2 WAL 写入 100,000 个顺序 versioned patch/put record，重建 Projector 后 Flush；断言：

```go
if got := finalVersion(t, collection, id); got != 100000 { t.Fatalf("version=%d", got) }
if got := transactionReceiptCount(t); got != 100000 { t.Fatalf("receipts=%d", got) }
```

记录 `records/sec`、总耗时和 WAL bytes；测试不设置硬编码容量通过阈值，只要求 10 分钟 deadline 内语义正确。

- [x] **Step 6: 运行完整真实门禁两次**

Run: `scripts/integration/dataengine-env.sh test && scripts/integration/dataengine-env.sh test`

Expected: 两次 PASS，第二次证明环境和测试幂等；结束后 `status` 为全健康。

- [x] **Step 7: 提交故障门禁**

```bash
git add dataengine/failover_integration_test.go nestwal/backlog_integration_test.go scripts/integration
git commit -m "test(dataengine): gate failover and backlog recovery"
```

### Task 4: 将剩余 dirty/scope 使用者迁入 Data Engine

**Files:**
- Modify: `roost-core/entity/entity.go`
- Modify: `roost-core/nest/rollback.go`
- Modify: `roost-core/nest/nest_test.go`
- Modify: `roost-core/entity/example_gen_test.go`
- Modify: `roost-skill/combatcomponent/component.go`
- Modify: `roost-skill/combatcomponent/component_test.go`

**Interfaces:**
- Consumes: `dataengine.Tracker`, `dataengine.DatabaseScope`, `nest.MarkPersist`
- Produces: no production import of `cube-core/checkpoint` outside the package scheduled for deletion

- [x] **Step 1: 写 architecture/import 和 CombatDao mutation 失败测试**

在 core 测试扫描非 `checkpoint/` Go 文件，发现 `cube-core/checkpoint` import 时失败。Combat 测试在 Nest transaction 内调用 mutator，断言 transaction change mask 和 sync mask 都包含目标字段；事务外持久化 mutation 必须 panic/返回 `nest.ErrPersistenceOutsideTransaction`。

- [x] **Step 2: 运行并确认旧 import/dirty 行为导致失败**

Run:

```bash
go test ./dataengine ./entity ./nest -count=1
go test ./combatcomponent -count=1
```

Expected: FAIL on checkpoint imports and missing transaction-local persist change.

- [x] **Step 3: 迁移 core entity/nest**

`DatabaseScopedDao.DbScope()` 改为 `dataengine.DatabaseScope`；`RollbackTx.captureDao` 只捕获 `*dataengine.Tracker` 和 `TrackerSnapshot`，删除 legacy `DirtyTracker` 分支。测试 DAO 改用 `dataengine.Tracker`。

- [x] **Step 4: 迁移 CombatDao**

`CombatDao.dirty` 改为 `dataengine.Tracker`，`DirtyTracker()` 返回该类型；`markDirty` 实现：

```go
if err := nest.MarkPersist(component.dao, mask); err != nil {
    panic(fmt.Errorf("combatcomponent: mark persistence: %w", err))
}
component.dao.dirty.MarkSync(mask)
```

实现 `nest.MutationParticipant` 所需的 Put/Patch/Delete preparation，复用当前 JSON persisted payload 时使用 Put；后续字段 BSON patch 由独立生成 DAO 承担，不为手写 combat state 引入反射 patch。

- [x] **Step 5: 运行 core/skill 测试并提交**

```bash
go test ./dataengine ./entity ./nest -count=1
go test ./combatcomponent ./... -count=1
git -C roost-core add entity nest dataengine
git -C roost-core commit -m "refactor(dataengine): remove legacy dirty contracts"
git -C roost-skill add combatcomponent
git -C roost-skill commit -m "refactor(combat): persist through data engine transactions"
```

### Task 5: 删除 core Checkpoint 写包

**Files:**
- Delete: `roost-core/checkpoint/`
- Modify: `roost-core/README.md`
- Modify: `roost-core/PRODUCTION_READINESS.md`
- Modify: `roost-core/NEST_TRANSACTION_WAL.md`
- Modify: `roost-core/docs/DATA_ENGINE_MIGRATION.md`
- Create: `roost-core/dataengine/architecture_test.go`

**Interfaces:**
- Produces: `cube-core/checkpoint` package no longer exists
- Preserves: unrelated skill runtime “checkpoint” terminology and NestWAL ack checkpoint terminology

- [x] **Step 1: 写禁止 Checkpoint package/legacy symbols 的失败测试**

`dataengine/architecture_test.go` 定位 module root 并断言 `checkpoint` directory 不存在，同时扫描 `.go` 文件禁止：`SnapshotWAL`、`EntitySnapshotter`、`RemoveSnapshot`、`TakePersistDirty`、`RollbackPersist`。

- [x] **Step 2: 运行并确认 checkpoint directory 导致失败**

Run: `go test ./dataengine -run TestLegacyCheckpointWritePathIsAbsent -count=1`

Expected: FAIL naming `checkpoint` directory.

- [x] **Step 3: 删除 package 并更新文档**

删除 `roost-core/checkpoint` 全目录；README 示例改为 Data Engine Tracker/Nest transaction，迁移文档将 Checkpoint 标记为已删除的历史来源，不留下启用步骤。不要误删 `syncstream` 和 `nestwal` 的 ack checkpoint，它们是 WAL watermark，不是 Entity 第二写路径。

- [x] **Step 4: 运行全量 core/skill 测试和引用扫描**

Run:

```bash
go test ./... -count=1
go test -race ./dataengine ./entity ./nest ./saga -count=1
rg -n 'cube-core/checkpoint|SnapshotWAL|EntitySnapshotter|RemoveSnapshot' . --glob '*.go'
```

Expected: tests PASS；rg 无结果。

- [x] **Step 5: 提交 core 删除**

```bash
git add -A
git commit -m "refactor(dataengine): remove checkpoint write engine"
```

### Task 6: 删除 kit legacy Checkpoint/NestWAL runtime 并强制 Data Engine

**Files:**
- Delete: `checkpoint/`
- Delete: `nestwal/mod.go`
- Delete: `nestwal/mod_options_test.go`
- Delete: `nestwal/mongo_atomic_applier.go`
- Delete: `nestwal/pilot_test.go`
- Modify: `mods/persistence.go`
- Modify: `mods/persistence_test.go`
- Modify: `mods/name.go`
- Modify: `dataengine/mod_test.go`
- Modify: `nest/nest_mod.go`
- Modify: `nest/nest_mod_test.go`
- Modify: `saga/mod.go`
- Modify: `lifecycle_contract_test.go`
- Modify: `README.md`

**Interfaces:**
- Produces: `persistence.engine` defaults to and only accepts `dataengine`
- Produces: Nest and Saga depend on `ModDataEngine`; `nestwal` remains a physical WAL library only

- [x] **Step 1: 将 persistence tests 改为只接受 Data Engine，并确认失败**

```go
func TestPersistenceEngineOnlyAcceptsDataEngine(t *testing.T) {
    for _, value := range []string{"", "dataengine"} { /* expect DataEngineEnabled */ }
    for _, value := range []string{"checkpoint", "nestwal"} { /* expect ErrPersistenceEngineSelection */ }
}
```

Run: `go test ./mods ./dataengine ./nest ./saga -count=1`

Expected: FAIL because empty still defaults checkpoint and modules reference legacy capabilities.

- [x] **Step 2: 简化 persistence selection 和模块依赖**

删除 `PersistenceCheckpoint`、`CheckpointEnabled`、`ModCheckpoint`、`ModNestWAL`。`ResolvePersistenceEngine` 对空值和 `dataengine` 返回 `{Engine:"dataengine",DataEngineEnabled:true}`；设置 `checkpoint.enabled`、`dataengine.enabled=false` 或其他 engine 时返回明确迁移错误。

Nest `Provide` 只从 `ModDataEngine` 取得 lazy committer；Saga optional dependency 只保留 `ModDataEngine`。

- [x] **Step 3: 删除 legacy runtime files 并更新 contract tests/docs**

删除 kit checkpoint package和只把 NestWAL 接到 Checkpoint MongoBackend 的文件；保留 `nestwal.WAL`、codec、ack checkpoint、committer primitives。README 将 `checkpoint.*` 配置和旧 capability 表移除。

- [x] **Step 4: 运行 kit 全量、race、vet 和真实门禁**

```bash
go test ./... -count=1
go test -race ./dataengine ./nest ./nestwal ./remote_entity ./saga -count=1
go vet ./dataengine ./nest ./nestwal ./remote_entity ./saga ./mods
scripts/integration/dataengine-env.sh test
```

Expected: PASS.

- [x] **Step 5: 提交 kit 删除**

```bash
git add -A
git commit -m "refactor(dataengine): remove legacy checkpoint runtime"
```

### Task 7: 删除 codegen Checkpoint 选项并将默认工程切到 Data Engine

**Files:**
- Modify: `internal/roost/catalog.go`
- Modify: `internal/roost/render.go`
- Modify: `internal/roost/add_workflow.go`
- Modify: `internal/roost/render_workflow_docs.go`
- Modify: `internal/roost/roost_test.go`
- Modify: `internal/entity/parse.go`
- Modify: `internal/entity/testdata/player.go`
- Modify: `internal/dao/parse_test.go`
- Modify: `README.md`
- Modify: `docs/NEST_RUNTIME.zh-CN.md`
- Modify: `docs/CODEGEN_REFERENCE.zh-CN.md`

**Interfaces:**
- Produces: 新项目始终 scaffold `dataengine`，不存在 `checkpoint`/standalone `nestwal` mod
- Consumes: workspace-local core/kit Data Engine packages

- [x] **Step 1: 更新 generator tests，先要求无 legacy 输出**

在 project/workflow tests 对所有生成文件扫描并禁止：

```go
for _, forbidden := range []string{"kitcheckpoint", "checkpoint.NewMod", "- checkpoint", "- nestwal", "cube-core/checkpoint"} {
    if strings.Contains(generated, forbidden) { t.Fatalf("legacy persistence output %q", forbidden) }
}
```

同时断言默认 `roost.yaml` 含 `- dataengine`，lifecycle 使用 `kitdataengine.ErrEntityAggregateNotFound`。

- [x] **Step 2: 运行并确认旧 catalog/render 导致失败**

Run: `go test ./internal/roost ./internal/entity ./internal/dao -count=1`

Expected: FAIL on legacy output.

- [x] **Step 3: 删除 legacy catalog/render/parser compatibility**

移除 checkpoint 和 standalone nestwal catalog entries、互斥分支和发布依赖 gate；默认 preset 直接选择 dataengine。Entity/DAO testdata 改用 `dataengine.Tracker`，parser 不再特殊识别 checkpoint import。

- [x] **Step 4: 更新文档并运行全量 codegen 测试**

Run:

```bash
go test ./... -count=1
go vet ./internal/dao ./internal/entity ./internal/roost
rg -n 'cube-core/checkpoint|cube-kit/checkpoint|kitcheckpoint|Snapshot\(\)|RemoveSnapshot' . --glob '*.go'
```

Expected: tests/vet PASS；rg 只允许用于“禁止旧输出”的测试字符串。

- [x] **Step 5: 提交 codegen 删除**

```bash
git add -A
git commit -m "refactor(codegen): make data engine the only persistence path"
```

### Task 8: 最终跨仓验证和交付说明

**Files:**
- Modify: `docs/superpowers/specs/2026-09-01-dataengine-local-integration-design.md`
- Modify: `docs/superpowers/plans/2026-09-01-dataengine-integration-and-checkpoint-removal.md`
- Modify: `roost-core/docs/DATA_ENGINE_MIGRATION.md`

**Interfaces:**
- Produces: 可复现命令、实际本机拓扑、100k 恢复结果和已删除 legacy allowlist

- [x] **Step 1: 执行四仓全量测试**

```bash
git -C ../roost-core status --short && (cd ../roost-core && go test ./... -count=1)
go test ./... -count=1
(cd ../roost-skill && go test ./... -count=1)
(cd ../roost-codegen && go test ./... -count=1)
```

- [x] **Step 2: 执行 race、vet、真实门禁和 architecture scan**

```bash
(cd ../roost-core && go test -race ./dataengine ./entity ./nest ./saga -count=1)
go test -race ./dataengine ./nest ./nestwal ./remote_entity ./saga -count=1
go vet ./dataengine ./nest ./nestwal ./remote_entity ./saga ./mods
scripts/integration/dataengine-env.sh test
rg -n 'cube-(core|kit)/checkpoint|SnapshotWAL|RedisSnapshotWAL|EntitySnapshotter|RemoveSnapshot' ../roost-core . ../roost-skill ../roost-codegen --glob '*.go'
```

Expected: tests/race/vet/integration PASS；scan 仅命中禁止旧符号的 architecture test 字符串。

- [x] **Step 3: 记录实测结果并更新计划状态**

文档记录 Mongo/NATS 版本、端口、failover 用时、100k 恢复耗时和吞吐；把已完成 checkbox 标为 `[x]`，不得把本机功能测试写成生产容量认证。

- [x] **Step 4: 分仓提交文档并确认工作区**

```bash
git add docs scripts
git commit -m "docs(dataengine): record integration and checkpoint removal"
git -C ../roost-core status --short
git status --short
git -C ../roost-skill status --short
git -C ../roost-codegen status --short
```

Expected: 四个仓库均在 `main`，无未提交文件；根 `go.work` 仍解析四个本地 module。
