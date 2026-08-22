package checkpoint

import (
	"context"
	"errors"
	"testing"

	corecheckpoint "github.com/tjbdwanghaibo/cube-core/checkpoint"
	fmongo "github.com/tjbdwanghaibo/cube-core/mongo"
	"go.mongodb.org/mongo-driver/v2/bson"
)

type checkpointMongoFake struct{ db *checkpointDatabaseFake }

func (m *checkpointMongoFake) Database(string) fmongo.IDatabase              { return m.db }
func (m *checkpointMongoFake) DatabaseForSid(string, int32) fmongo.IDatabase { return m.db }
func (m *checkpointMongoFake) StartSession(context.Context) (fmongo.ISession, error) {
	return nil, errors.New("unused")
}
func (m *checkpointMongoFake) Ping(context.Context) error  { return nil }
func (m *checkpointMongoFake) Close(context.Context) error { return nil }

type checkpointDatabaseFake struct{ coll *checkpointCollectionFake }

func (d *checkpointDatabaseFake) Name() string                         { return "game" }
func (d *checkpointDatabaseFake) Collection(string) fmongo.ICollection { return d.coll }
func (d *checkpointDatabaseFake) Drop(context.Context) error           { return nil }

type checkpointCollectionFake struct {
	matched    int64
	bulkCalls  int
	upsertErrs map[int64]error
	raw        [][]byte
}

func (c *checkpointCollectionFake) InsertOne(context.Context, any) (string, error) { panic("unused") }
func (c *checkpointCollectionFake) InsertMany(context.Context, []any) ([]string, error) {
	panic("unused")
}
func (c *checkpointCollectionFake) FindOne(context.Context, any, any) error { panic("unused") }
func (c *checkpointCollectionFake) Find(context.Context, any, any, ...fmongo.FindOption) error {
	panic("unused")
}
func (c *checkpointCollectionFake) UpdateOne(context.Context, any, any) (*fmongo.UpdateResult, error) {
	panic("unused")
}
func (c *checkpointCollectionFake) UpdateMany(context.Context, any, any) (*fmongo.UpdateResult, error) {
	panic("unused")
}
func (c *checkpointCollectionFake) ReplaceOne(context.Context, any, any) (*fmongo.UpdateResult, error) {
	panic("unused")
}
func (c *checkpointCollectionFake) DeleteOne(context.Context, any) (int64, error)  { panic("unused") }
func (c *checkpointCollectionFake) DeleteMany(context.Context, any) (int64, error) { return 0, nil }
func (c *checkpointCollectionFake) FindOneAndUpdate(_ context.Context, filter any, _ any, _ any, _ ...fmongo.FindOneAndUpdateOption) error {
	id := filter.(bson.M)["_id"].(int64)
	return c.upsertErrs[id]
}
func (c *checkpointCollectionFake) FindOneAndDelete(context.Context, any, any) error { panic("unused") }
func (c *checkpointCollectionFake) FindOneAndReplace(context.Context, any, any, any) error {
	panic("unused")
}
func (c *checkpointCollectionFake) CountDocuments(context.Context, any) (int64, error) {
	panic("unused")
}
func (c *checkpointCollectionFake) Aggregate(context.Context, any, any) error { panic("unused") }
func (c *checkpointCollectionFake) BulkWrite(_ context.Context, models []fmongo.WriteModel) (*fmongo.BulkWriteResult, error) {
	c.bulkCalls++
	return &fmongo.BulkWriteResult{MatchedCount: c.matched}, nil
}
func (c *checkpointCollectionFake) EnsureIndexes(context.Context, []fmongo.IndexModel) error {
	return nil
}
func (c *checkpointCollectionFake) StreamFind(_ context.Context, _ any, consume func([]byte) error, _ ...fmongo.FindOption) error {
	for _, raw := range c.raw {
		if err := consume(raw); err != nil {
			return err
		}
	}
	return nil
}

func checkpointBSON(t *testing.T, id int64) []byte {
	t.Helper()
	raw, err := bson.Marshal(bson.M{"_id": id, "name": "player"})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestMongoBackendBulkSaveFastPathAndExactFallback(t *testing.T) {
	collection := &checkpointCollectionFake{matched: 2, upsertErrs: map[int64]error{}}
	backend, err := NewMongoBackend(&checkpointMongoFake{db: &checkpointDatabaseFake{coll: collection}}, MongoBackendConfig{DefaultDatabase: "game", ServerID: 1})
	if err != nil {
		t.Fatal(err)
	}
	ops := []corecheckpoint.SaveOp{
		{Collection: "players", ID: 1, Version: 1, Data: checkpointBSON(t, 1)},
		{Collection: "players", ID: 2, Version: 1, Data: checkpointBSON(t, 2)},
	}
	results, err := backend.BulkSave(context.Background(), ops)
	if err != nil || !results[0].OK || !results[1].OK || collection.bulkCalls != 1 {
		t.Fatalf("fast results=%+v calls=%d err=%v", results, collection.bulkCalls, err)
	}

	collection.matched = 0
	collection.upsertErrs[2] = fmongo.ErrDuplicateKey
	results, err = backend.BulkSave(context.Background(), ops)
	if err != nil || !results[0].OK || !results[1].VersionConflict {
		t.Fatalf("fallback results=%+v err=%v", results, err)
	}
}

func TestMongoBackendStreamLoadCopiesMetadataAndPayload(t *testing.T) {
	raw, err := bson.Marshal(bson.M{"_id": int64(7), "_version": uint64(9), "_schema": uint32(3), "name": "player"})
	if err != nil {
		t.Fatal(err)
	}
	collection := &checkpointCollectionFake{raw: [][]byte{raw}}
	backend, err := NewMongoBackend(&checkpointMongoFake{db: &checkpointDatabaseFake{coll: collection}}, MongoBackendConfig{DefaultDatabase: "game"})
	if err != nil {
		t.Fatal(err)
	}
	var loaded corecheckpoint.RawDoc
	err = backend.StreamLoad(context.Background(), corecheckpoint.LoadOp{Collection: "players", BatchSize: 50}, func(doc corecheckpoint.RawDoc) error {
		loaded = doc
		return nil
	})
	if err != nil || loaded.ID != 7 || loaded.Version != 9 || loaded.SchemaVersion != 3 || len(loaded.Data) == 0 {
		t.Fatalf("loaded=%+v err=%v", loaded, err)
	}
}

func TestMongoBackendRejectsUnsafePatchPath(t *testing.T) {
	op := corecheckpoint.SaveOp{ID: 1, Version: 1, Mode: corecheckpoint.SaveModePatch, Patch: corecheckpoint.PersistPatch{Set: map[string]any{"profile.$bad": 1}}}
	if _, err := patchUpdate(op); err == nil {
		t.Fatal("unsafe patch path was accepted")
	}
}
