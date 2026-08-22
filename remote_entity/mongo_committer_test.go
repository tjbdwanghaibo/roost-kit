package remote_entity

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/tjbdwanghaibo/cube-core/entity"
	fmongo "github.com/tjbdwanghaibo/cube-core/mongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type remoteMongoFake struct {
	mu  sync.Mutex
	dbs map[string]*remoteMongoDBFake
}

func newRemoteMongoFake() *remoteMongoFake {
	return &remoteMongoFake{dbs: make(map[string]*remoteMongoDBFake)}
}
func (m *remoteMongoFake) Database(name string) fmongo.IDatabase {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.dbs[name] == nil {
		m.dbs[name] = &remoteMongoDBFake{name: name, collections: make(map[string]*remoteMongoCollectionFake)}
	}
	return m.dbs[name]
}
func (m *remoteMongoFake) DatabaseForSid(prefix string, sid int32) fmongo.IDatabase {
	return m.Database(fmt.Sprintf("%s_%d", prefix, sid))
}
func (*remoteMongoFake) StartSession(context.Context) (fmongo.ISession, error) {
	return remoteMongoSessionFake{}, nil
}
func (*remoteMongoFake) Ping(context.Context) error  { return nil }
func (*remoteMongoFake) Close(context.Context) error { return nil }

type remoteMongoSessionFake struct{}

func (remoteMongoSessionFake) WithTransaction(ctx context.Context, fn func(context.Context) error) error {
	return fn(ctx)
}
func (remoteMongoSessionFake) EndSession(context.Context) {}

type remoteMongoDBFake struct {
	name        string
	mu          sync.Mutex
	collections map[string]*remoteMongoCollectionFake
}

func (d *remoteMongoDBFake) Name() string { return d.name }
func (d *remoteMongoDBFake) Collection(name string) fmongo.ICollection {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.collections[name] == nil {
		d.collections[name] = &remoteMongoCollectionFake{docs: make(map[string]bson.M)}
	}
	return d.collections[name]
}
func (*remoteMongoDBFake) Drop(context.Context) error { return nil }

type remoteMongoCollectionFake struct {
	fmongo.ICollection
	mu      sync.Mutex
	docs    map[string]bson.M
	indexes []fmongo.IndexModel
}

func documentMap(doc any) (bson.M, error) {
	raw, err := bson.Marshal(doc)
	if err != nil {
		return nil, err
	}
	var result bson.M
	err = bson.Unmarshal(raw, &result)
	return result, err
}

func docKey(id any) string { return fmt.Sprint(id) }

func (c *remoteMongoCollectionFake) InsertOne(_ context.Context, doc any) (string, error) {
	value, err := documentMap(doc)
	if err != nil {
		return "", err
	}
	key := docKey(value["_id"])
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.docs[key]; ok {
		return "", fmongo.ErrDuplicateKey
	}
	c.docs[key] = value
	return key, nil
}

func (c *remoteMongoCollectionFake) UpdateOne(_ context.Context, filter any, update any) (*fmongo.UpdateResult, error) {
	f, _ := documentMap(filter)
	u, _ := documentMap(update)
	key := docKey(f["_id"])
	c.mu.Lock()
	defer c.mu.Unlock()
	doc, ok := c.docs[key]
	if !ok {
		return &fmongo.UpdateResult{}, nil
	}
	if expected, exists := f["_ver"]; exists && numeric(doc["_ver"]) != numeric(expected) {
		return &fmongo.UpdateResult{}, nil
	}
	if state, exists := f["state"]; exists && numeric(doc["state"]) != numeric(state) {
		return &fmongo.UpdateResult{}, nil
	}
	if set, ok := asBSONMap(u["$set"]); ok {
		for k, v := range set {
			doc[k] = v
		}
	}
	c.docs[key] = doc
	return &fmongo.UpdateResult{MatchedCount: 1, ModifiedCount: 1}, nil
}

func (c *remoteMongoCollectionFake) BulkWrite(_ context.Context, models []fmongo.WriteModel) (*fmongo.BulkWriteResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, model := range models {
		value, err := documentMap(model.Document)
		if err != nil {
			return nil, err
		}
		c.docs[docKey(value["_id"])] = value
	}
	return &fmongo.BulkWriteResult{ModifiedCount: int64(len(models))}, nil
}

func (c *remoteMongoCollectionFake) DeleteOne(_ context.Context, filter any) (int64, error) {
	f, _ := documentMap(filter)
	key := docKey(f["_id"])
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.docs[key]; !ok {
		return 0, nil
	}
	delete(c.docs, key)
	return 1, nil
}

func (c *remoteMongoCollectionFake) FindOne(_ context.Context, filter any, result any) error {
	f, _ := documentMap(filter)
	key := docKey(f["_id"])
	c.mu.Lock()
	doc, ok := c.docs[key]
	c.mu.Unlock()
	if !ok {
		return fmongo.ErrNotFound
	}
	if min, ok := asBSONMap(f["state_version"]); ok && numeric(doc["state_version"]) < numeric(min["$gte"]) {
		return fmongo.ErrNotFound
	}
	raw, _ := bson.Marshal(doc)
	return bson.Unmarshal(raw, result)
}

func (c *remoteMongoCollectionFake) Find(_ context.Context, filter any, results any, opts ...fmongo.FindOption) error {
	f, _ := documentMap(filter)
	c.mu.Lock()
	values := make([]bson.M, 0, len(c.docs))
	for _, doc := range c.docs {
		if state, ok := f["state"]; ok && numeric(doc["state"]) != numeric(state) {
			continue
		}
		values = append(values, doc)
	}
	c.mu.Unlock()
	if len(opts) > 0 && opts[0].Limit > 0 && int64(len(values)) > opts[0].Limit {
		values = values[:opts[0].Limit]
	}
	target, ok := results.(*[]mongoRemoteTransaction)
	if !ok {
		return fmt.Errorf("unsupported fake Find result %T", results)
	}
	for _, value := range values {
		raw, err := bson.Marshal(value)
		if err != nil {
			return err
		}
		var decoded mongoRemoteTransaction
		if err := bson.Unmarshal(raw, &decoded); err != nil {
			return err
		}
		*target = append(*target, decoded)
	}
	return nil
}

func (c *remoteMongoCollectionFake) EnsureIndexes(_ context.Context, indexes []fmongo.IndexModel) error {
	c.mu.Lock()
	c.indexes = append([]fmongo.IndexModel(nil), indexes...)
	c.mu.Unlock()
	return nil
}

func numeric(value any) uint64 {
	switch v := value.(type) {
	case int32:
		return uint64(v)
	case int64:
		return uint64(v)
	case uint32:
		return uint64(v)
	case uint64:
		return v
	case float64:
		return uint64(v)
	default:
		return 0
	}
}

func asBSONMap(value any) (bson.M, bool) {
	switch v := value.(type) {
	case bson.M:
		return v, true
	case bson.D:
		result := make(bson.M, len(v))
		for _, item := range v {
			result[item.Key] = item.Value
		}
		return result, true
	default:
		return nil, false
	}
}

func TestMongoCommitterCASIdempotencySnapshotAndOutbox(t *testing.T) {
	const kind entity.EntityKind = 198
	entity.MustRegisterEntityKindDefs(entity.EntityKindDef{Kind: kind, Category: 1, RemotePolicy: entity.RemotePolicyManaged})
	id, err := entity.BuildEntityID(991, kind)
	if err != nil {
		t.Fatal(err)
	}
	var tx entity.RemoteTransactionID
	tx[15] = 1
	key := entity.RemoteSnapshotKey{EntityID: id, Kind: kind, Scope: 1}
	data := []byte("state")
	commit := entity.RemoteCommit{
		TransactionID: tx, EntityID: id, Kind: kind, BaseVersion: 0, NextVersion: 1,
		MarkerEpoch: 1, RouteEpoch: 1, Schema: 1, Codec: 1,
		Mutations: []entity.RemoteDataMutation{{Database: "game", Collection: "players", ID: id, Version: 1, Data: data}},
		Snapshots: []entity.RemoteSnapshotRecord{{Key: key, BaseVersion: 0, StateVersion: 1, MarkerEpoch: 1, RouteEpoch: 1, Schema: 1, Codec: 1, Full: true, Data: data, Checksum: entity.RemoteSnapshotChecksum(data)}},
	}
	mongo := newRemoteMongoFake()
	store := NewMongoCommitter(mongo, "control", 7, 0)
	if err := store.EnsureRemoteStorage(context.Background()); err != nil {
		t.Fatal(err)
	}
	txCollection := mongo.Database("control").Collection(remoteTxCollection).(*remoteMongoCollectionFake)
	if len(txCollection.indexes) != 2 || !txCollection.indexes[1].Sparse || !txCollection.indexes[1].RecreateOnConflict || txCollection.indexes[1].TTL <= 0 {
		t.Fatalf("unsafe transaction TTL indexes: %+v", txCollection.indexes)
	}
	first, err := store.CommitRemote(context.Background(), commit)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.CommitRemote(context.Background(), commit)
	if err != nil || second != first {
		t.Fatalf("idempotent receipt=%+v first=%+v err=%v", second, first, err)
	}
	altered := commit.Clone()
	altered.Mutations[0].Data = []byte("different")
	if _, err := store.CommitRemote(context.Background(), altered); !errors.Is(err, entity.ErrRemoteRejected) {
		t.Fatalf("transaction id content collision error=%v", err)
	}
	pending, err := store.PendingRemoteCommits(context.Background(), 10)
	if err != nil || len(pending) != 1 || len(pending[0].Commits) != 1 {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}
	txCollection.mu.Lock()
	_, expiresBeforePublish := txCollection.docs[tx.String()]["expires_at"]
	txCollection.mu.Unlock()
	if expiresBeforePublish {
		t.Fatal("unpublished outbox transaction must not expire")
	}
	snapshot, ok, err := store.LoadRemoteSnapshot(context.Background(), key, entity.RemoteReadLinearizable, 1)
	if err != nil || !ok || string(snapshot.Payload.BytesCopy()) != "state" {
		t.Fatalf("snapshot=%+v ok=%v err=%v", snapshot, ok, err)
	}
	if err := store.MarkRemoteCommitPublished(context.Background(), tx); err != nil {
		t.Fatal(err)
	}
	txCollection.mu.Lock()
	_, expires := txCollection.docs[tx.String()]["expires_at"]
	txCollection.mu.Unlock()
	if !expires {
		t.Fatal("published transaction must receive expires_at")
	}
	pending, err = store.PendingRemoteCommits(context.Background(), 10)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pending after ack=%+v err=%v", pending, err)
	}

	stale := commit.Clone()
	stale.TransactionID[14] = 2
	if _, err := store.CommitRemote(context.Background(), stale); !errors.Is(err, entity.ErrRemoteVersionConflict) {
		t.Fatalf("stale error=%v", err)
	}
	duplicate := commit.Clone()
	duplicate.TransactionID[13] = 3
	if _, err := store.CommitRemoteBatch(context.Background(), []entity.RemoteCommit{duplicate, duplicate}); !errors.Is(err, entity.ErrRemoteRejected) {
		t.Fatalf("duplicate entity batch error=%v", err)
	}
}
