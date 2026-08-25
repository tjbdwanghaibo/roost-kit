package checkpoint

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	corecheckpoint "github.com/tjbdwanghaibo/cube-core/checkpoint"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const repositoryTestKind entity.EntityKind = 238

type repositoryTestDAO struct {
	id      int64
	Version uint64
}

func (d *repositoryTestDAO) Id() int64          { return d.id }
func (d *repositoryTestDAO) SetId(id int64)     { d.id = id }
func (*repositoryTestDAO) DbName() string       { return "game" }
func (*repositoryTestDAO) CollName() string     { return "repository_entities" }
func (*repositoryTestDAO) Dirty() entity.IDirty { return nil }
func (*repositoryTestDAO) CleanDirty()          {}
func (d *repositoryTestDAO) RestorePersisted(raw []byte, _ uint32, version uint64) error {
	var doc struct {
		ID int64 `bson:"_id"`
	}
	if err := bson.Unmarshal(raw, &doc); err != nil {
		return err
	}
	d.id, d.Version = doc.ID, version
	return nil
}

type repositoryTestEntity struct {
	*entity.EntityBase
	dao *repositoryTestDAO
}

func (e *repositoryTestEntity) Base() *entity.EntityBase                     { return e.EntityBase }
func (e *repositoryTestEntity) OnInitFinish(*entity.EntityCreateParam) error { return nil }
func (e *repositoryTestEntity) OnDestroy(entity.EntityDestroyReason)         {}
func (*repositoryTestEntity) AutoPersist() bool                              { return true }
func (*repositoryTestEntity) IsRemoved() bool                                { return false }
func (*repositoryTestEntity) SetRemoved()                                    {}
func (*repositoryTestEntity) Touch() bool                                    { return true }
func (*repositoryTestEntity) UnTouch()                                       {}
func (*repositoryTestEntity) ClearBase()                                     {}
func (*repositoryTestEntity) IsClear() bool                                  { return false }
func (e *repositoryTestEntity) RemoveSnapshot() []corecheckpoint.SaveItem {
	return []corecheckpoint.SaveItem{{Db: "game", Collection: "repository_entities", ID: e.ID(), Version: 1, Deleted: true}}
}

var registerRepositoryEntity sync.Once

func ensureRepositoryEntity() {
	registerRepositoryEntity.Do(func() {
		entity.RegisterEntityBuilder(&entity.EntityBuilderParam{
			Category: 1, Kind: repositoryTestKind,
			DaoBuilders: []entity.DaoBuilderFunc{func() entity.DaoInterface { return &repositoryTestDAO{} }},
			Builder: func(param *entity.EntityCreateParam) (entity.IThreadSafeEntity, error) {
				return &repositoryTestEntity{EntityBase: entity.NewEntityBaseWithMutex(param.Id, param.Category, false, param.Mutex, param.Kind), dao: param.Dao["repository_entities"].(*repositoryTestDAO)}, nil
			},
		})
	})
}

type repositoryReader struct {
	doc          corecheckpoint.RawDoc
	transactions atomic.Int32
}

func (r *repositoryReader) ReadConsistent(ctx context.Context, fn func(context.Context) error) error {
	r.transactions.Add(1)
	time.Sleep(20 * time.Millisecond)
	return fn(ctx)
}
func (r *repositoryReader) BulkLoad(context.Context, corecheckpoint.LoadOp) ([]corecheckpoint.RawDoc, error) {
	return []corecheckpoint.RawDoc{r.doc}, nil
}

func TestEntityRepositorySingleFlightsAndPublishesCompleteAggregate(t *testing.T) {
	ensureRepositoryEntity()
	id, err := entity.BuildEntityID(991, repositoryTestKind)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := bson.Marshal(bson.M{"_id": id, "_schema": uint32(1)})
	reader := &repositoryReader{doc: corecheckpoint.RawDoc{ID: id, Version: 7, SchemaVersion: 1, Data: raw}}
	repository, err := newEntityRepository(entity.NewEntityManager(), reader)
	if err != nil {
		t.Fatal(err)
	}

	const callers = 24
	results := make(chan entity.IThreadSafeEntity, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, loadErr := repository.LoadEntity(context.Background(), id, repositoryTestKind)
			results <- value
			errs <- loadErr
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for loadErr := range errs {
		if loadErr != nil {
			t.Fatal(loadErr)
		}
	}
	var first entity.IThreadSafeEntity
	for value := range results {
		if first == nil {
			first = value
		}
		if value != first {
			t.Fatal("singleflight published different entity instances")
		}
	}
	if reader.transactions.Load() != 1 {
		t.Fatalf("snapshot transactions=%d want=1", reader.transactions.Load())
	}
	if first.(*repositoryTestEntity).dao.Version != 7 {
		t.Fatalf("DAO version was not restored")
	}
}
