package checkpoint

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	corecheckpoint "github.com/tjbdwanghaibo/cube-core/checkpoint"
	fmongo "github.com/tjbdwanghaibo/cube-core/mongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const nestTransactionCollection = "_nest_transactions"

var ErrAtomicTransactionIdentity = errors.New("checkpoint mongo: transaction identity conflict")

type MongoBackendConfig struct {
	DefaultDatabase       string
	ServerID              int32
	MaxConcurrentGroups   int
	TransactionReceiptTTL time.Duration
}

// MongoBackend persists checkpoint snapshots with version-based CAS. The hot
// steady-state path is one BulkWrite per database/collection. Only batches
// containing a missing or stale document fall back to per-document upserts so
// result classification remains exact.
type MongoBackend struct {
	client fmongo.IMongo
	cfg    MongoBackendConfig
}

func NewMongoBackend(client fmongo.IMongo, cfg MongoBackendConfig) (*MongoBackend, error) {
	if client == nil {
		return nil, corecheckpoint.ErrCheckpointBackendRequired
	}
	cfg.DefaultDatabase = strings.TrimSpace(cfg.DefaultDatabase)
	if cfg.DefaultDatabase == "" {
		return nil, fmt.Errorf("checkpoint mongo: default database is required")
	}
	if cfg.MaxConcurrentGroups <= 0 {
		cfg.MaxConcurrentGroups = 8
	}
	if cfg.TransactionReceiptTTL <= 0 {
		cfg.TransactionReceiptTTL = 30 * 24 * time.Hour
	}
	return &MongoBackend{client: client, cfg: cfg}, nil
}

// EnsureInfrastructure installs lifecycle indexes before traffic is accepted.
func (b *MongoBackend) EnsureInfrastructure(ctx context.Context) error {
	if b == nil || b.client == nil {
		return corecheckpoint.ErrCheckpointBackendRequired
	}
	ttlSeconds := int64(b.cfg.TransactionReceiptTTL / time.Second)
	if ttlSeconds <= 0 || ttlSeconds > int64(^uint32(0)>>1) {
		return fmt.Errorf("checkpoint mongo: invalid transaction receipt ttl %s", b.cfg.TransactionReceiptTTL)
	}
	return b.client.Database(b.cfg.DefaultDatabase).Collection(nestTransactionCollection).EnsureIndexes(ctx, []fmongo.IndexModel{{
		Keys: bson.D{{Key: "created_at", Value: 1}}, Name: "ttl_created_at", TTL: int32(ttlSeconds),
	}})
}

type saveGroupKey struct {
	db           string
	serverScoped bool
	collection   string
}

type indexedSave struct {
	index int
	op    corecheckpoint.SaveOp
}

type nestTransactionReceipt struct {
	ID        string    `bson:"_id"`
	Digest    []byte    `bson:"digest"`
	CreatedAt time.Time `bson:"created_at"`
}

// ApplyAtomicTransaction commits every ordinary mutation and the optional
// participant in one MongoDB session transaction. The receipt is written in
// that same transaction, making WAL replay exactly-once by transaction ID.
// The participant must only use the supplied transaction context and must not
// start another MongoDB session.
func (b *MongoBackend) ApplyAtomicTransaction(ctx context.Context, transactionID string, digest []byte, ops []corecheckpoint.SaveOp, participant func(context.Context) error) error {
	if b == nil || b.client == nil || transactionID == "" || len(digest) == 0 {
		return fmt.Errorf("%w: missing transaction id or digest", ErrAtomicTransactionIdentity)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	for i := range ops {
		if err := validateSaveOp(ops[i]); err != nil {
			return fmt.Errorf("checkpoint mongo: atomic mutation %d: %w", i, err)
		}
	}
	session, err := b.client.StartSession(ctx)
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)
	return session.WithTransaction(ctx, func(txCtx context.Context) error {
		var receipt nestTransactionReceipt
		err := b.client.Database(b.cfg.DefaultDatabase).Collection(nestTransactionCollection).FindOne(txCtx, bson.M{"_id": transactionID}, &receipt)
		if err == nil {
			if !bytes.Equal(receipt.Digest, digest) {
				return ErrAtomicTransactionIdentity
			}
			return nil
		}
		if !errors.Is(err, fmongo.ErrNotFound) {
			return err
		}
		if err := b.applySaveOpsInTransaction(txCtx, ops); err != nil {
			return err
		}
		if participant != nil {
			if err := participant(txCtx); err != nil {
				return err
			}
		}
		_, err = b.client.Database(b.cfg.DefaultDatabase).Collection(nestTransactionCollection).InsertOne(txCtx, nestTransactionReceipt{
			ID: transactionID, Digest: append([]byte(nil), digest...), CreatedAt: time.Now().UTC(),
		})
		return err
	})
}

// AcknowledgeAtomicTransaction removes an idempotency receipt only after the
// corresponding WAL checkpoint has durably advanced. Failure is safe and only
// leaks a small receipt; callers may retry cleanup independently.
func (b *MongoBackend) AcknowledgeAtomicTransaction(ctx context.Context, transactionID string) error {
	if b == nil || b.client == nil || transactionID == "" {
		return nil
	}
	_, err := b.client.Database(b.cfg.DefaultDatabase).Collection(nestTransactionCollection).DeleteOne(ctx, bson.M{"_id": transactionID})
	return err
}

// ReadConsistent runs an aggregate read in one MongoDB snapshot transaction.
// It is used by the entity repository so DAOs from different collections can
// never be assembled from different committed points in time.
func (b *MongoBackend) ReadConsistent(ctx context.Context, read func(context.Context) error) error {
	if b == nil || b.client == nil || read == nil {
		return corecheckpoint.ErrCheckpointBackendRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	session, err := b.client.StartSession(ctx)
	if err != nil {
		return err
	}
	defer session.EndSession(ctx)
	return session.WithTransaction(ctx, read)
}

func (b *MongoBackend) applySaveOpsInTransaction(ctx context.Context, ops []corecheckpoint.SaveOp) error {
	type orderedSave struct {
		index int
		op    corecheckpoint.SaveOp
	}
	ordered := make([]orderedSave, len(ops))
	for i := range ops {
		ordered[i] = orderedSave{index: i, op: ops[i]}
	}
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i].op, ordered[j].op
		if left.Db != right.Db {
			return left.Db < right.Db
		}
		if left.DbScope != right.DbScope {
			return left.DbScope < right.DbScope
		}
		if left.Collection != right.Collection {
			return left.Collection < right.Collection
		}
		return left.ID < right.ID
	})
	for _, item := range ordered {
		op := item.op
		coll := b.collection(b.databaseName(op.Db), op.DbScope == corecheckpoint.DatabaseScopeServer, op.Collection)
		result, err := b.saveOne(ctx, coll, op)
		if err != nil {
			return fmt.Errorf("checkpoint mongo: atomic mutation %d: %w", item.index, err)
		}
		if result.VersionConflict {
			return fmt.Errorf("checkpoint mongo: atomic mutation %d: %w", item.index, fmongo.ErrVersionConflict)
		}
		if !result.OK {
			if result.Err != nil {
				return fmt.Errorf("checkpoint mongo: atomic mutation %d: %w", item.index, result.Err)
			}
			return fmt.Errorf("checkpoint mongo: atomic mutation %d was not applied", item.index)
		}
	}
	return nil
}

func (b *MongoBackend) BulkSave(ctx context.Context, ops []corecheckpoint.SaveOp) ([]corecheckpoint.SaveResult, error) {
	results := make([]corecheckpoint.SaveResult, len(ops))
	if len(ops) == 0 {
		return results, nil
	}
	groups := make(map[saveGroupKey][]indexedSave)
	for i, op := range ops {
		if err := validateSaveOp(op); err != nil {
			results[i] = corecheckpoint.SaveResult{Err: err}
			continue
		}
		name := b.databaseName(op.Db)
		key := saveGroupKey{db: name, serverScoped: op.DbScope == corecheckpoint.DatabaseScopeServer, collection: op.Collection}
		groups[key] = append(groups[key], indexedSave{index: i, op: op})
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	sem := make(chan struct{}, b.cfg.MaxConcurrentGroups)
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var groupErr error
	for key, group := range groups {
		key, group := key, group
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			if err := b.saveGroup(ctx, key, group, results); err != nil {
				errMu.Lock()
				groupErr = errors.Join(groupErr, err)
				errMu.Unlock()
				cancel()
			}
		}()
	}
	wg.Wait()
	if groupErr != nil {
		return nil, groupErr
	}
	return results, nil
}

func (b *MongoBackend) saveGroup(ctx context.Context, key saveGroupKey, group []indexedSave, results []corecheckpoint.SaveResult) error {
	coll := b.collection(key.db, key.serverScoped, key.collection)
	models := make([]fmongo.WriteModel, 0, len(group))
	valid := make([]indexedSave, 0, len(group))
	for _, item := range group {
		model, err := saveWriteModel(item.op, false)
		if err != nil {
			results[item.index] = corecheckpoint.SaveResult{Err: err}
			continue
		}
		models = append(models, model)
		valid = append(valid, item)
	}
	if len(models) == 0 {
		return nil
	}
	bulk, err := coll.BulkWrite(ctx, models)
	if err != nil {
		return fmt.Errorf("checkpoint mongo: bulk save %s/%s: %w", key.db, key.collection, err)
	}
	if bulk.MatchedCount == int64(len(valid)) {
		for _, item := range valid {
			results[item.index] = corecheckpoint.SaveResult{OK: true}
		}
		return nil
	}
	// A no-match can mean either a missing document or a newer stored version.
	// Upsert each item to classify that ambiguity without a read/write race.
	for _, item := range valid {
		result, err := b.saveOne(ctx, coll, item.op)
		if err != nil {
			return fmt.Errorf("checkpoint mongo: save %s/%s/%d: %w", key.db, key.collection, item.op.ID, err)
		}
		results[item.index] = result
	}
	return nil
}

func (b *MongoBackend) saveOne(ctx context.Context, coll fmongo.ICollection, op corecheckpoint.SaveOp) (corecheckpoint.SaveResult, error) {
	filter := saveFilter(op)
	raw := op.Data
	if op.Mode == corecheckpoint.SaveModePatch && len(op.Patch.FullData) > 0 {
		raw = op.Patch.FullData
	}
	doc, err := replacementDocument(op, raw)
	if err != nil {
		return corecheckpoint.SaveResult{Err: err}, nil
	}
	update := bson.M{"$set": cloneDocumentWithoutID(doc), "$setOnInsert": bson.M{"_id": op.ID}}
	var after bson.M
	err = coll.FindOneAndUpdate(ctx, filter, update, &after, fmongo.FindOneAndUpdateOption{Upsert: true, ReturnAfter: true})
	if errors.Is(err, fmongo.ErrDuplicateKey) {
		stale, classifyErr := storedVersionAtLeast(ctx, coll, op.ID, op.Version)
		if classifyErr == nil && stale {
			return corecheckpoint.SaveResult{VersionConflict: true}, nil
		}
		if classifyErr != nil {
			return corecheckpoint.SaveResult{}, errors.Join(err, fmt.Errorf("classify duplicate: %w", classifyErr))
		}
		return corecheckpoint.SaveResult{}, err
	}
	if err != nil {
		return corecheckpoint.SaveResult{}, err
	}
	return corecheckpoint.SaveResult{OK: true}, nil
}

func saveWriteModel(op corecheckpoint.SaveOp, upsert bool) (fmongo.WriteModel, error) {
	filter := saveFilter(op)
	if op.Mode == corecheckpoint.SaveModePatch && !op.Patch.Empty() {
		update, err := patchUpdate(op)
		if err != nil {
			return fmongo.WriteModel{}, err
		}
		return fmongo.NewUpdateOneModel(filter, update, upsert), nil
	}
	doc, err := replacementDocument(op, op.Data)
	if err != nil {
		return fmongo.WriteModel{}, err
	}
	return fmongo.NewReplaceOneModel(filter, doc, upsert), nil
}

func validateSaveOp(op corecheckpoint.SaveOp) error {
	if op.Collection == "" || op.ID == 0 || op.Version == 0 {
		return fmt.Errorf("checkpoint mongo: invalid save identity")
	}
	if op.Mode == corecheckpoint.SaveModePatch && !op.Patch.Empty() {
		if len(op.Patch.FullData) == 0 && len(op.Data) == 0 {
			return fmt.Errorf("checkpoint mongo: patch has no full fallback")
		}
		return nil
	}
	if len(op.Data) == 0 {
		return fmt.Errorf("checkpoint mongo: empty full snapshot")
	}
	return nil
}

func saveFilter(op corecheckpoint.SaveOp) bson.M {
	clauses := bson.A{
		bson.M{"$or": bson.A{bson.M{"_version": bson.M{"$lt": op.Version}}, bson.M{"_version": bson.M{"$exists": false}}}},
	}
	if op.Fence > 0 {
		clauses = append(clauses, bson.M{"$or": bson.A{
			bson.M{"_fence": bson.M{"$lt": op.Fence}},
			bson.M{"_fence": op.Fence, "_owner_sid": op.OwnerSid},
			bson.M{"_fence": bson.M{"$exists": false}},
		}})
	}
	return bson.M{"_id": op.ID, "$and": clauses}
}

func replacementDocument(op corecheckpoint.SaveOp, raw []byte) (bson.M, error) {
	var doc bson.M
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("checkpoint mongo: decode full snapshot: %w", err)
	}
	doc["_id"] = op.ID
	doc["_version"] = op.Version
	delete(doc, "_deleted")
	delete(doc, "_deleted_at")
	if op.Fence > 0 {
		doc["_fence"], doc["_owner_sid"], doc["_shared"] = op.Fence, op.OwnerSid, op.Shared
	}
	return doc, nil
}

func patchUpdate(op corecheckpoint.SaveOp) (bson.M, error) {
	set := bson.M{"_version": op.Version}
	if op.Fence > 0 {
		set["_fence"], set["_owner_sid"], set["_shared"] = op.Fence, op.OwnerSid, op.Shared
	}
	for path, value := range op.Patch.Set {
		if !validPatchPath(path) {
			return nil, fmt.Errorf("checkpoint mongo: invalid patch path %q", path)
		}
		set[path] = value
	}
	update := bson.M{"$set": set}
	// A strictly newer save is the only supported explicit re-creation path.
	// Clear tombstone metadata even for a patch update.
	unset := bson.M{"_deleted": "", "_deleted_at": ""}
	if len(op.Patch.Unset) > 0 {
		for _, path := range op.Patch.Unset {
			if !validPatchPath(path) {
				return nil, fmt.Errorf("checkpoint mongo: invalid unset path %q", path)
			}
			unset[path] = ""
		}
	}
	update["$unset"] = unset
	return update, nil
}

func validPatchPath(path string) bool {
	if path == "" || strings.ContainsRune(path, '\x00') {
		return false
	}
	for _, segment := range strings.Split(path, ".") {
		if segment == "" || strings.HasPrefix(segment, "$") {
			return false
		}
	}
	return true
}

func cloneDocumentWithoutID(doc bson.M) bson.M {
	set := make(bson.M, len(doc))
	for key, value := range doc {
		if key != "_id" {
			set[key] = value
		}
	}
	return set
}

func (b *MongoBackend) BulkLoad(ctx context.Context, op corecheckpoint.LoadOp) ([]corecheckpoint.RawDoc, error) {
	docs := make([]corecheckpoint.RawDoc, 0)
	err := b.StreamLoad(ctx, op, func(doc corecheckpoint.RawDoc) error {
		docs = append(docs, doc)
		return nil
	})
	return docs, err
}

func (b *MongoBackend) StreamLoad(ctx context.Context, op corecheckpoint.LoadOp, consume func(corecheckpoint.RawDoc) error) error {
	if op.Collection == "" || consume == nil {
		return fmt.Errorf("checkpoint mongo: invalid load request")
	}
	coll := b.collection(b.databaseName(op.Db), op.DbScope == corecheckpoint.DatabaseScopeServer, op.Collection)
	filter := activeDocumentFilter(op.Filter)
	stream, ok := coll.(fmongo.IStreamingCollection)
	if !ok {
		var raw []bson.Raw
		if err := coll.Find(ctx, filter, &raw, fmongo.FindOption{BatchSize: int32(op.BatchSize)}); err != nil {
			return err
		}
		for _, item := range raw {
			if err := consumeRawDoc(item, consume); err != nil {
				return err
			}
		}
		return nil
	}
	return stream.StreamFind(ctx, filter, func(raw []byte) error {
		return consumeRawDoc(bson.Raw(raw), consume)
	}, fmongo.FindOption{BatchSize: int32(op.BatchSize)})
}

func consumeRawDoc(raw bson.Raw, consume func(corecheckpoint.RawDoc) error) error {
	var meta struct {
		ID            int64   `bson:"_id"`
		Version       uint64  `bson:"_version"`
		RemoteVersion *uint64 `bson:"_ver"`
		Schema        uint32  `bson:"_schema"`
		MarkerEpoch   uint64  `bson:"_marker_epoch"`
		LockFence     uint64  `bson:"_lock_fence"`
		RouteEpoch    uint64  `bson:"_route_epoch"`
		Deleted       bool    `bson:"_deleted"`
	}
	if err := bson.Unmarshal(raw, &meta); err != nil {
		return err
	}
	if meta.ID == 0 {
		return fmt.Errorf("checkpoint mongo: loaded document has zero _id")
	}
	enveloped := meta.RemoteVersion != nil
	if enveloped {
		meta.Version = *meta.RemoteVersion
	}
	return consume(corecheckpoint.RawDoc{
		ID: meta.ID, Version: meta.Version, SchemaVersion: meta.Schema,
		MarkerEpoch: meta.MarkerEpoch, LockFence: meta.LockFence, RouteEpoch: meta.RouteEpoch,
		DataEnvelope: enveloped, Deleted: meta.Deleted, Data: append([]byte(nil), raw...),
	})
}

func (b *MongoBackend) BulkRemove(ctx context.Context, op corecheckpoint.RemoveOp) error {
	if op.Collection == "" || len(op.Items) == 0 {
		return nil
	}
	coll := b.collection(b.databaseName(op.Db), op.DbScope == corecheckpoint.DatabaseScopeServer, op.Collection)
	deletedAt := time.Now().UTC()
	for i := range op.Items {
		item := op.Items[i]
		if item.ID == 0 || item.Version == 0 {
			return fmt.Errorf("checkpoint mongo: invalid delete identity at %d", i)
		}
		set := bson.M{"_version": item.Version, "_deleted": true, "_deleted_at": deletedAt}
		if item.Fence > 0 {
			set["_fence"], set["_owner_sid"], set["_shared"] = item.Fence, item.OwnerSid, item.Shared
		}
		var after bson.M
		err := coll.FindOneAndUpdate(ctx, removeFilter(item), bson.M{"$set": set, "$setOnInsert": bson.M{"_id": item.ID}}, &after, fmongo.FindOneAndUpdateOption{Upsert: true, ReturnAfter: true})
		if errors.Is(err, fmongo.ErrDuplicateKey) {
			stale, classifyErr := storedVersionAtLeast(ctx, coll, item.ID, item.Version)
			if classifyErr == nil && stale {
				// A newer save/tombstone already owns this identity.
				continue
			}
			if classifyErr != nil {
				return errors.Join(err, fmt.Errorf("checkpoint mongo: classify tombstone duplicate %s/%d: %w", op.Collection, item.ID, classifyErr))
			}
		}
		if err != nil {
			return fmt.Errorf("checkpoint mongo: tombstone %s/%d: %w", op.Collection, item.ID, err)
		}
	}
	return nil
}

func storedVersionAtLeast(ctx context.Context, coll fmongo.ICollection, id int64, version uint64) (bool, error) {
	var current struct {
		Version uint64 `bson:"_version"`
	}
	if err := coll.FindOne(ctx, bson.M{"_id": id}, &current); err != nil {
		return false, err
	}
	return current.Version >= version, nil
}

func removeFilter(item corecheckpoint.RemoveItem) bson.M {
	clauses := bson.A{bson.M{"$or": bson.A{
		bson.M{"_version": bson.M{"$lt": item.Version}},
		bson.M{"_version": bson.M{"$exists": false}},
	}}}
	if item.Fence > 0 {
		clauses = append(clauses, bson.M{"$or": bson.A{
			bson.M{"_fence": bson.M{"$lt": item.Fence}},
			bson.M{"_fence": item.Fence, "_owner_sid": item.OwnerSid},
			bson.M{"_fence": bson.M{"$exists": false}},
		}})
	}
	return bson.M{"_id": item.ID, "$and": clauses}
}

func activeDocumentFilter(filter map[string]any) bson.M {
	active := bson.M{"_deleted": bson.M{"$ne": true}}
	if len(filter) == 0 {
		return active
	}
	return bson.M{"$and": bson.A{bson.M(filter), active}}
}

func (b *MongoBackend) databaseName(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		return b.cfg.DefaultDatabase
	}
	return name
}

func (b *MongoBackend) collection(database string, serverScoped bool, collection string) fmongo.ICollection {
	if serverScoped {
		return b.client.DatabaseForSid(database, b.cfg.ServerID).Collection(collection)
	}
	return b.client.Database(database).Collection(collection)
}

var _ corecheckpoint.StorageBackend = (*MongoBackend)(nil)
var _ corecheckpoint.StreamingStorageBackend = (*MongoBackend)(nil)
