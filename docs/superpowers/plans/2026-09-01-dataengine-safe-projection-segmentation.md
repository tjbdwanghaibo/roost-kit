# Data Engine Safe Projection Segmentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve Data Engine durability and Saga semantics while splitting mixed WAL replay batches into ordered projection segments with segment-level acknowledgement and record/byte bounds.

**Architecture:** A package-private pure planner classifies a replayed prefix into consecutive ordinary and special segments without reordering. Projector executes one segment at a time, acknowledges only that successful segment, and stops before later segments on Mongo or WAL failure; MongoStore keeps its existing transaction and marker formats.

**Tech Stack:** Go 1.26.5, cube-core Data Engine/Nest models, roost-kit segmented WAL, MongoDB driver v2, Viper, build-tagged integration tests.

**Spec:** `docs/superpowers/specs/2026-09-01-dataengine-projection-segmentation-design.md`

## Global Constraints

- WAL remains the only durable admission path; no legacy Checkpoint write path may return.
- Mongo projection must succeed before the matching WAL fence is acknowledged.
- Only a continuous successful WAL prefix may be acknowledged; an ack failure stops later segments.
- Same-entity mutations retain WAL order and exact version CAS.
- Multi-document mutations, receipts, effects, remote mutations, and Saga steps retain their Mongo transaction boundary.
- Mongo collection/index/marker formats remain unchanged.
- A one-record ordinary segment retains the non-transactional single-document fast path.
- Projector parallelism, Patch compaction, Outbox/Saga concurrency changes, and WAL format changes are out of scope.

---

### Task 1: Pure projection segment planner

**Files:**
- Create: `dataengine/projection_plan.go`
- Create: `dataengine/projection_plan_test.go`

**Interfaces:**
- Consumes: `coredata.CommitRecord`, `corenest.CommitFence`, `MigrationHandler`.
- Produces: `projectionSegment`, `isBatchProjectionRecord(coredata.CommitRecord) bool`, `projectionRecordLogicalBytes(coredata.CommitRecord) int`, `planProjectionSegments([]coredata.CommitRecord, []corenest.CommitFence, int, int) ([]projectionSegment, error)`.

- [ ] **Step 1: Write failing eligibility and order tests**

```go
func TestPlanProjectionSegmentsPreservesMixedOrder(t *testing.T) {
    records := []coredata.CommitRecord{
        projectorRecord(1, false), projectorRecord(2, false),
        projectorRecord(3, true), projectorRecord(4, false),
    }
    fences := projectionTestFences(records)
    segments, err := planProjectionSegments(records, fences, 16, 4<<20)
    if err != nil { t.Fatal(err) }
    if len(segments) != 3 || !segments[0].batch || segments[1].batch || !segments[2].batch {
        t.Fatalf("segments=%+v", segments)
    }
    if len(segments[0].records) != 2 || segments[1].records[0].ID != records[2].ID {
        t.Fatalf("segment records=%+v", segments)
    }
}

func TestBatchProjectionEligibilityMatchesMongoContract(t *testing.T) {
    ordinary := projectorRecord(1, false)
    if !isBatchProjectionRecord(ordinary) { t.Fatal("ordinary record not batchable") }
    for name, edit := range map[string]func(*coredata.CommitRecord){
        "migration": func(r *coredata.CommitRecord) { r.Handler = MigrationHandler },
        "effect": func(r *coredata.CommitRecord) { r.Effects = []coredata.Effect{{ID: "e", Topic: "t"}} },
        "receipt": func(r *coredata.CommitRecord) { r.Receipts = []coredata.Receipt{{Namespace: "n", ID: "r"}} },
        "multiple": func(r *coredata.CommitRecord) { r.Mutations = append(r.Mutations, r.Mutations[0]) },
    } {
        t.Run(name, func(t *testing.T) {
            record := projectorRecord(1, false)
            edit(&record)
            if isBatchProjectionRecord(record) { t.Fatal("special record is batchable") }
        })
    }
}
```

- [ ] **Step 2: Run tests and verify RED**

Run: `go test ./dataengine -run 'TestPlanProjectionSegments|TestBatchProjectionEligibility' -count=1`

Expected: build failure because planner symbols do not exist.

- [ ] **Step 3: Implement minimal planner**

```go
type projectionSegment struct {
    records []coredata.CommitRecord
    fences  []corenest.CommitFence
    batch   bool
}

func isBatchProjectionRecord(record coredata.CommitRecord) bool {
    return record.Handler != MigrationHandler && len(record.Mutations) == 1 &&
        len(record.Effects) == 0 && len(record.Receipts) == 0 && record.Mutations[0].Remote == nil
}
```

`planProjectionSegments` must reject mismatched slices and non-positive limits, preserve order, create one-record special segments, and split consecutive ordinary records at either `maxRecords` or `maxBytes`. An oversized first ordinary record forms one segment so planning always advances. `projectionRecordLogicalBytes` must use saturating addition and sum fixed overhead plus string/byte lengths from metadata, mutation/Patch, Effect, Receipt, and Remote fields without serialization.

- [ ] **Step 4: Add boundary tests**

```go
func TestPlanProjectionSegmentsBoundsOrdinaryBatches(t *testing.T) {
    records := []coredata.CommitRecord{projectorRecord(1, false), projectorRecord(2, false), projectorRecord(3, false)}
    fences := projectionTestFences(records)
    one := projectionRecordLogicalBytes(records[0])
    segments, err := planProjectionSegments(records, fences, 2, one*2-1)
    if err != nil { t.Fatal(err) }
    if len(segments) != 3 { t.Fatalf("segments=%d", len(segments)) }
}

func TestPlanProjectionSegmentsRejectsMismatchedFences(t *testing.T) {
    if _, err := planProjectionSegments([]coredata.CommitRecord{projectorRecord(1, false)}, nil, 16, 4<<20); err == nil {
        t.Fatal("expected record/fence mismatch")
    }
}
```

Also test oversized-record progress, Remote rejection, no dropped/duplicated IDs, and zero limits.

- [ ] **Step 5: Verify GREEN**

Run: `go test ./dataengine -run 'TestPlanProjectionSegments|TestBatchProjectionEligibility' -count=1`

Run: `go test ./dataengine -count=1`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dataengine/projection_plan.go dataengine/projection_plan_test.go
git commit -m "feat(dataengine): plan bounded projection segments"
```

### Task 2: Segment execution and continuous WAL acknowledgement

**Files:**
- Modify: `dataengine/projector.go`
- Modify: `dataengine/projector_test.go`

**Interfaces:**
- Consumes: Task 1 planner and existing `ProjectionStore`/`BatchProjectionStore`.
- Produces: `ProjectorOptions.ReplayBatchBytes int` and per-segment acknowledgement.

- [ ] **Step 1: Write failing successful-prefix regression**

```go
type recordingSegmentStore struct {
    events []string
    failID coredata.TransactionID
}

func (store *recordingSegmentStore) Project(_ context.Context, record coredata.CommitRecord) error {
    store.events = append(store.events, "project:"+record.ID.String())
    if record.ID == store.failID { return context.DeadlineExceeded }
    return nil
}

func (store *recordingSegmentStore) ProjectBatch(_ context.Context, records []coredata.CommitRecord) error {
    store.events = append(store.events, fmt.Sprintf("batch:%d", len(records)))
    return nil
}

func TestProjectorAcknowledgesSuccessfulPrefixBeforeLaterSegmentFailure(t *testing.T) {
    options := nestwal.DefaultOptions(t.TempDir())
    options.WriterVersion = nestwal.WriterVersionV2
    wal, err := nestwal.Open(options)
    if err != nil { t.Fatal(err) }
    defer wal.Close(context.Background())
    records := []coredata.CommitRecord{
        projectorRecord(1, false), projectorRecord(2, false),
        projectorRecord(3, true), projectorRecord(4, false),
    }
    store := &recordingSegmentStore{failID: records[2].ID}
    projector, err := NewProjector(wal, store, ProjectorOptions{
        ReplayBatchRecords: 16, ReplayBatchBytes: 4 << 20, CloseWAL: false, IdlePoll: time.Hour,
    })
    if err != nil { t.Fatal(err) }
    projector.cancel()
    <-projector.done
    for i := range records {
        if _, err := wal.Append(context.Background(), records[i]); err != nil { t.Fatal(err) }
    }

processed, err := projector.replayPass(context.Background())
if !errors.Is(err, context.DeadlineExceeded) || processed != 2 {
    t.Fatalf("processed=%d err=%v", processed, err)
}
var remaining []coredata.TransactionID
if err := wal.Replay(context.Background(), func(_ corenest.CommitFence, record coredata.CommitRecord) error {
    remaining = append(remaining, record.ID)
    return nil
}); err != nil { t.Fatal(err) }
want := []coredata.TransactionID{records[2].ID, records[3].ID}
if !slices.Equal(remaining, want) { t.Fatalf("remaining=%v want=%v", remaining, want) }
if !slices.Equal(store.events, []string{"batch:2", "project:" + records[2].ID.String()}) {
    t.Fatalf("events=%v", store.events)
}
}
```

- [ ] **Step 2: Verify RED**

Run: `go test ./dataengine -run TestProjectorAcknowledgesSuccessfulPrefixBeforeLaterSegmentFailure -count=1`

Expected: FAIL because the whole replay batch remains unacknowledged.

- [ ] **Step 3: Implement segment execution**

After WAL replay, call the planner with `ReplayBatchRecords` and `ReplayBatchBytes`. For each segment: project it, complete its projection tickets and increment `Projected`, ack its last fence, then remove admitted IDs and continue. Return immediately on Mongo or ack failure. Count only successfully projected records in the returned `processed` value, including a segment whose Mongo commit succeeded but ack failed.

Add `ReplayBatchBytes int` to `ProjectorOptions`, set it to `4 << 20` in `DefaultProjectorOptions`, and apply that default in `NewProjector` when the configured value is non-positive.

Add a private `ack func(context.Context, corenest.CommitFence) error` field to Projector. `NewProjector` binds it to `wal.Ack`; tests may replace it to reproduce an acknowledgement failure after a successful Mongo call without changing the public API.

`projectSegment` calls `ProjectBatch` only when `len(segment.records) > 1`; a one-record ordinary or special segment calls `Project`. A planned multi-record segment returning `errProjectionBatchUnsupported` is treated as rule drift and returned before any later segment is processed.

- [ ] **Step 4: Add fast-path and byte-split tests**

```go
func stoppedProjectorWithRecords(t *testing.T, store ProjectionStore, records []coredata.CommitRecord, batchBytes int) (*Projector, *nestwal.WAL) {
    t.Helper()
    options := nestwal.DefaultOptions(t.TempDir())
    options.WriterVersion = nestwal.WriterVersionV2
    wal, err := nestwal.Open(options)
    if err != nil { t.Fatal(err) }
    t.Cleanup(func() { _ = wal.Close(context.Background()) })
    projector, err := NewProjector(wal, store, ProjectorOptions{
        ReplayBatchRecords: 16, ReplayBatchBytes: batchBytes, CloseWAL: false, IdlePoll: time.Hour,
    })
    if err != nil { t.Fatal(err) }
    projector.cancel()
    <-projector.done
    for i := range records {
        if _, err := wal.Append(context.Background(), records[i]); err != nil { t.Fatal(err) }
    }
    return projector, wal
}

func assertWALReplayCount(t *testing.T, wal *nestwal.WAL, want int) {
    t.Helper()
    got := 0
    if err := wal.Replay(context.Background(), func(corenest.CommitFence, coredata.CommitRecord) error {
        got++
        return nil
    }); err != nil { t.Fatal(err) }
    if got != want { t.Fatalf("replay records=%d want=%d", got, want) }
}

func TestProjectorKeepsSingleOrdinarySegmentsOnProjectFastPath(t *testing.T) {
    records := []coredata.CommitRecord{projectorRecord(1, false), projectorRecord(2, true), projectorRecord(3, false)}
    store := &recordingSegmentStore{}
    projector, wal := stoppedProjectorWithRecords(t, store, records, 4<<20)
    processed, err := projector.replayPass(context.Background())
    if err != nil || processed != 3 { t.Fatalf("processed=%d err=%v", processed, err) }
    want := []string{
        "project:" + records[0].ID.String(), "project:" + records[1].ID.String(), "project:" + records[2].ID.String(),
    }
    if !slices.Equal(store.events, want) { t.Fatalf("events=%v want=%v", store.events, want) }
    assertWALReplayCount(t, wal, 0)
}

func TestProjectorSplitsOrdinaryBatchAtLogicalByteLimit(t *testing.T) {
    records := []coredata.CommitRecord{
        projectorRecord(1, false), projectorRecord(2, false), projectorRecord(3, false), projectorRecord(4, false),
    }
    store := &recordingSegmentStore{}
    limit := projectionRecordLogicalBytes(records[0]) + projectionRecordLogicalBytes(records[1])
    projector, wal := stoppedProjectorWithRecords(t, store, records, limit)
    processed, err := projector.replayPass(context.Background())
    if err != nil || processed != 4 { t.Fatalf("processed=%d err=%v", processed, err) }
    if !slices.Equal(store.events, []string{"batch:2", "batch:2"}) { t.Fatalf("events=%v", store.events) }
    assertWALReplayCount(t, wal, 0)
}
```

The assertions must inspect real call counts and WAL replay state, not mocks of planner internals.

Add an acknowledgement-failure test with two ordinary records followed by one special record:

```go
func TestProjectorStopsAfterSegmentAckFailure(t *testing.T) {
    records := []coredata.CommitRecord{projectorRecord(1, false), projectorRecord(2, false), projectorRecord(3, true)}
    store := &recordingSegmentStore{}
    projector, _ := stoppedProjectorWithRecords(t, store, records, 4<<20)
    ackErr := errors.New("checkpoint unavailable")
    projector.ack = func(context.Context, corenest.CommitFence) error { return ackErr }
    processed, err := projector.replayPass(context.Background())
    if !errors.Is(err, ackErr) || processed != 2 { t.Fatalf("processed=%d err=%v", processed, err) }
    if !slices.Equal(store.events, []string{"batch:2"}) { t.Fatalf("events=%v", store.events) }
}
```

- [ ] **Step 5: Verify GREEN and regressions**

Run: `go test ./dataengine -run 'TestProjector(Acknowledges|Keeps|Splits|UsesAtomic)' -count=1`

Run: `go test ./dataengine -count=1`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add dataengine/projector.go dataengine/projector_test.go
git commit -m "fix(dataengine): ack projection segments continuously"
```

### Task 3: Configuration and documentation

**Files:**
- Modify: `dataengine/mod.go`
- Modify: `dataengine/mod_test.go`
- Modify: `README.md`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `ProjectorOptions.ReplayBatchBytes`.
- Produces: `dataengine.projection.batch_bytes`, default 4 MiB.

- [ ] **Step 1: Write failing config test**

```go
func TestDataEngineModReadsProjectionBatchByteLimit(t *testing.T) {
    cfg := viper.New()
    cfg.Set("persistence.engine", "dataengine")
    cfg.Set("dataengine.projection.batch_bytes", 2<<20)
    mod := NewMod(WithEntityAccess(entity.NewManagerAccess(entity.NewEntityManager())))
    if err := mod.Init(cfg); err != nil { t.Fatal(err) }
    if got := mod.cfg.projector.ReplayBatchBytes; got != 2<<20 {
        t.Fatalf("projection batch bytes=%d", got)
    }
}
```

- [ ] **Step 2: Verify RED**

Run: `go test ./dataengine -run TestDataEngineModReadsProjectionBatchByteLimit -count=1`

Expected: assertion/build failure before config wiring.

- [ ] **Step 3: Wire and document**

```go
if value := cfg.GetInt("dataengine.projection.batch_bytes"); value > 0 {
    projector.ReplayBatchBytes = value
}
```

README must state that record and logical-byte caps apply simultaneously, mixed replay keeps WAL order, segment ack never crosses a failure, and a single ordinary record keeps the fast path. CHANGELOG must record the partial-replay correctness fix and unchanged storage formats.

- [ ] **Step 4: Format and verify**

Run: `gofmt -w dataengine/projector.go dataengine/projection_plan.go dataengine/projection_plan_test.go dataengine/projector_test.go dataengine/mod.go dataengine/mod_test.go`

Run: `go test ./dataengine -count=1`

Run: `git diff --check`

Expected: all exit 0.

- [ ] **Step 5: Commit**

```bash
git add dataengine/mod.go dataengine/mod_test.go README.md CHANGELOG.md
git commit -m "docs(dataengine): expose projection byte bound"
```

### Task 4: Integration and performance evidence

**Files:**
- Modify: `dataengine/real_integration_test.go`
- Modify: `dataengine/projector_benchmark_test.go`
- Modify: `docs/superpowers/specs/2026-09-01-dataengine-projection-segmentation-design.md`

**Interfaces:**
- Consumes: segmented replay behavior.
- Produces: real mixed-order guard, mixed projection benchmark, and measured before/after results.

- [ ] **Step 1: Add real mixed projection guard**

```go
func TestRealMixedProjectionSegmentsPreserveOrder(t *testing.T) {
    fx := newRealFixture(t)
    defer fx.close()
    first := realRecord(41, []coredata.Mutation{
        realPut(t, fx.database, "mixed_entities", 4101, 0, 1, bson.M{"value": int64(1)}),
    })
    patch2, _ := bson.Marshal(bson.D{{Key: "value", Value: int64(2)}})
    second := realRecord(42, []coredata.Mutation{{
        Key: coredata.DocumentKey{Database: fx.database, Resource: "mixed_entities", ID: 4101},
        Kind: coredata.MutationPatch, ExpectedVersion: 1, NextVersion: 2, Schema: 1,
        Patch: coredata.FieldPatch{SetBSON: patch2},
    }})
    second.Receipts = []coredata.Receipt{{Namespace: "mixed", ID: "receipt-42", Digest: []byte("42")}}
    second.Effects = []coredata.Effect{{ID: "mixed-effect-42", Topic: "mixed.changed", Key: "4101"}}
    patch3, _ := bson.Marshal(bson.D{{Key: "value", Value: int64(3)}})
    third := realRecord(43, []coredata.Mutation{{
        Key: coredata.DocumentKey{Database: fx.database, Resource: "mixed_entities", ID: 4101},
        Kind: coredata.MutationPatch, ExpectedVersion: 2, NextVersion: 3, Schema: 1,
        Patch: coredata.FieldPatch{SetBSON: patch3},
    }})

    options := nestwal.DefaultOptions(t.TempDir())
    options.WriterVersion = nestwal.WriterVersionV2
    wal, err := nestwal.Open(options)
    if err != nil { t.Fatal(err) }
    defer wal.Close(context.Background())
    projector, err := NewProjector(wal, fx.runtime.Store, ProjectorOptions{CloseWAL: false, IdlePoll: time.Hour})
    if err != nil { t.Fatal(err) }
    projector.cancel()
    <-projector.done
    for _, record := range []coredata.CommitRecord{first, second, third} {
        if _, err := wal.Append(fx.context(), record); err != nil { t.Fatal(err) }
    }
    if processed, err := projector.replayPass(fx.context()); err != nil || processed != 3 {
        t.Fatalf("processed=%d err=%v", processed, err)
    }
    assertDocumentVersion(t, fx, "mixed_entities", 4101, 3)
    assertCollectionCount(t, fx, receiptCollection, 1)
    assertCollectionCount(t, fx, outboxCollection, 1)
    assertCollectionCount(t, fx, transactionCollection, 1)
    assertWALReplayCount(t, wal, 0)
}
```

- [ ] **Step 2: Run focused integration guard**

Run: `scripts/integration/dataengine-env.sh up`

Run: `source /private/tmp/roost-dataengine-it/env.sh && go test -tags=integration ./dataengine -run TestRealMixedProjectionSegmentsPreserveOrder -count=1 -v`

Expected: PASS. Task 2 supplies the mandatory RED proof for partial acknowledgement; this test proves real Mongo semantics.

- [ ] **Step 3: Add mixed replay benchmark**

```go
func BenchmarkProjectionSegmentPlanner(b *testing.B) {
    for _, specialEvery := range []int{0, 100, 10, 1} {
        name := fmt.Sprintf("special_every_%d", specialEvery)
        b.Run(name, func(b *testing.B) {
            records := make([]coredata.CommitRecord, 1024)
            for i := range records {
                records[i] = projectorRecord(byte(i%255+1), specialEvery > 0 && i%specialEvery == 0)
            }
            fences := projectionTestFences(records)
            b.ReportAllocs()
            b.ResetTimer()
            for range b.N {
                if _, err := planProjectionSegments(records, fences, 1024, 4<<20); err != nil { b.Fatal(err) }
            }
        })
    }
}
```

Run it with `go test ./dataengine -run '^$' -bench BenchmarkProjectionSegmentPlanner -benchmem -count=3` and retain all three samples.

- [ ] **Step 4: Run complete gates**

From `/Users/whb/roost`:

```bash
go test ./roost-core/... ./roost-kit/... ./roost-skill/... ./roost-codegen/...
go test -race ./roost-kit/dataengine ./roost-kit/nestwal ./roost-kit/saga
go vet ./roost-core/... ./roost-kit/... ./roost-skill/... ./roost-codegen/...
```

From `/Users/whb/roost/roost-kit`:

```bash
scripts/integration/dataengine-env.sh test
go test -tags=integration ./nestwal -run TestRealWALBacklogProjectsOneHundredThousandRecords -count=1 -v
go test -tags=integration ./dataengine -run TestRealSagaReceiptTransactionThroughput -count=1 -v
```

Expected: all exit 0; verbose logs include current backlog and Saga throughput.

- [ ] **Step 5: Record evidence and inspect state**

Append exact machine-local before/after measurements to the spec without calling them production capacity. Run `git diff --check`, `git status --short`, and `git log -8 --oneline --decorate`; only intended source, tests, docs, spec, and plan may differ.

- [ ] **Step 6: Commit**

```bash
git add dataengine/real_integration_test.go dataengine/projector_benchmark_test.go docs/superpowers/specs/2026-09-01-dataengine-projection-segmentation-design.md
git commit -m "test(dataengine): gate segmented projection recovery"
```
