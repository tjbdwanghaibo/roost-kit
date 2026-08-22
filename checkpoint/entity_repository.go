package checkpoint

import (
	"context"
	"errors"
	"fmt"
	"sync"

	corecheckpoint "github.com/tjbdwanghaibo/cube-core/checkpoint"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"go.mongodb.org/mongo-driver/v2/bson"
)

var (
	ErrEntityAggregateNotFound = errors.New("checkpoint repository: entity aggregate not found")
	ErrEntityAggregateCorrupt  = errors.New("checkpoint repository: entity aggregate is incomplete or corrupt")
)

type entityLoadFlight struct {
	done  chan struct{}
	value entity.IThreadSafeEntity
	err   error
}

// EntityRepository is the only cold-load path for runtime entities. It reads
// every DAO from one MongoDB snapshot transaction and single-flights concurrent
// misses by full entity ID before atomically publishing the complete aggregate.
type EntityRepository struct {
	manager *entity.EntityManager
	backend aggregateEntityReader

	flightMu sync.Mutex
	flights  map[int64]*entityLoadFlight
}

type aggregateEntityReader interface {
	ReadConsistent(context.Context, func(context.Context) error) error
	BulkLoad(context.Context, corecheckpoint.LoadOp) ([]corecheckpoint.RawDoc, error)
}

func NewEntityRepository(manager *entity.EntityManager, backend *MongoBackend) (*EntityRepository, error) {
	return newEntityRepository(manager, backend)
}

func newEntityRepository(manager *entity.EntityManager, backend aggregateEntityReader) (*EntityRepository, error) {
	if manager == nil || backend == nil {
		return nil, fmt.Errorf("checkpoint repository: entity manager and mongo backend are required")
	}
	return &EntityRepository{manager: manager, backend: backend, flights: make(map[int64]*entityLoadFlight)}, nil
}

func (r *EntityRepository) LoadEntity(ctx context.Context, id int64, kind entity.EntityKind) (entity.IThreadSafeEntity, error) {
	if r == nil || r.manager == nil || r.backend == nil {
		return nil, corecheckpoint.ErrCheckpointBackendRequired
	}
	if ctx == nil {
		ctx = context.Background()
	}
	fullID, err := entity.NormalizeFullID(id, kind)
	if err != nil {
		return nil, err
	}
	if loaded := r.manager.Get(fullID); loaded != nil {
		return loaded, nil
	}

	r.flightMu.Lock()
	if flight := r.flights[fullID]; flight != nil {
		r.flightMu.Unlock()
		select {
		case <-flight.done:
			return flight.value, flight.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	flight := &entityLoadFlight{done: make(chan struct{})}
	r.flights[fullID] = flight
	r.flightMu.Unlock()

	flight.value, flight.err = r.loadAggregate(ctx, fullID, kind)
	r.flightMu.Lock()
	delete(r.flights, fullID)
	close(flight.done)
	r.flightMu.Unlock()
	return flight.value, flight.err
}

func (r *EntityRepository) loadAggregate(ctx context.Context, fullID int64, kind entity.EntityKind) (entity.IThreadSafeEntity, error) {
	if loaded := r.manager.Get(fullID); loaded != nil {
		return loaded, nil
	}
	builder := entity.GetEntityBuilderParam(kind)
	if builder == nil {
		return nil, fmt.Errorf("checkpoint repository: no builder for entity kind %d", kind)
	}
	if builder.NoPersist || len(builder.DaoBuilders) == 0 {
		return nil, fmt.Errorf("%w: entity kind %d has no persistent DAO", ErrEntityAggregateNotFound, kind)
	}

	daos := make(map[string]entity.DaoInterface, len(builder.DaoBuilders))
	var remoteVector entity.RemoteVersionVector
	err := r.backend.ReadConsistent(ctx, func(txCtx context.Context) error {
		for index, buildDAO := range builder.DaoBuilders {
			if buildDAO == nil {
				return fmt.Errorf("%w: entity kind %d DAO builder %d is nil", ErrEntityAggregateCorrupt, kind, index)
			}
			dao := buildDAO()
			if dao == nil || dao.CollName() == "" {
				return fmt.Errorf("%w: entity kind %d DAO builder %d returned invalid DAO", ErrEntityAggregateCorrupt, kind, index)
			}
			if _, duplicate := daos[dao.CollName()]; duplicate {
				return fmt.Errorf("%w: duplicate DAO collection %q", ErrEntityAggregateCorrupt, dao.CollName())
			}
			docs, err := r.backend.BulkLoad(txCtx, corecheckpoint.LoadOp{
				Db: dao.DbName(), DbScope: corecheckpoint.ResolveDatabaseScope(dao),
				Collection: dao.CollName(), Filter: map[string]any{"_id": fullID}, BatchSize: 1,
			})
			if err != nil {
				return fmt.Errorf("checkpoint repository: load %s/%d: %w", dao.CollName(), fullID, err)
			}
			if len(docs) == 0 {
				return fmt.Errorf("%w: collection=%s entity=%d", ErrEntityAggregateNotFound, dao.CollName(), fullID)
			}
			if len(docs) != 1 || docs[0].ID != fullID {
				return fmt.Errorf("%w: collection=%s entity=%d documents=%d", ErrEntityAggregateCorrupt, dao.CollName(), fullID, len(docs))
			}
			doc := docs[0]
			payload, schema, err := persistedPayload(doc)
			if err != nil {
				return fmt.Errorf("%w: collection=%s entity=%d: %v", ErrEntityAggregateCorrupt, dao.CollName(), fullID, err)
			}
			hydrator, ok := dao.(entity.PersistedDaoLoader)
			if !ok {
				return fmt.Errorf("%w: collection=%s does not implement PersistedDaoLoader", ErrEntityAggregateCorrupt, dao.CollName())
			}
			if err := hydrator.RestorePersisted(payload, schema, doc.Version); err != nil {
				return fmt.Errorf("checkpoint repository: restore %s/%d: %w", dao.CollName(), fullID, err)
			}
			if dao.Id() != fullID {
				return fmt.Errorf("%w: collection=%s decoded id=%d want=%d", ErrEntityAggregateCorrupt, dao.CollName(), dao.Id(), fullID)
			}
			daos[dao.CollName()] = dao
			if doc.DataEnvelope && doc.Version >= remoteVector.StateVersion {
				remoteVector = entity.RemoteVersionVector{
					StateVersion: doc.Version, MarkerEpoch: doc.MarkerEpoch,
					LockFence: doc.LockFence, RouteEpoch: doc.RouteEpoch,
				}
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	param := &entity.EntityCreateParam{
		IsCreate: false, Category: builder.Category, Kind: kind, Id: fullID,
		Dao: daos, Lifetime: builder.Lifetime,
	}
	if builder.RemotePolicy.RemoteManaged() {
		if remoteVector.StateVersion == 0 {
			return nil, fmt.Errorf("%w: remote entity %d has no version envelope", ErrEntityAggregateCorrupt, fullID)
		}
		param.RemoteRestore = &remoteVector
	}
	loaded, err := r.manager.Create(param)
	if err != nil {
		if errors.Is(err, entity.ErrEntityExists) {
			if existing := r.manager.Get(fullID); existing != nil {
				return existing, nil
			}
		}
		return nil, err
	}
	return loaded, nil
}

func persistedPayload(doc corecheckpoint.RawDoc) ([]byte, uint32, error) {
	if !doc.DataEnvelope {
		return append([]byte(nil), doc.Data...), doc.SchemaVersion, nil
	}
	var envelope struct {
		Data []byte `bson:"data"`
	}
	if err := bson.Unmarshal(doc.Data, &envelope); err != nil {
		return nil, 0, err
	}
	if len(envelope.Data) == 0 {
		return nil, 0, fmt.Errorf("empty data envelope")
	}
	var innerMeta struct {
		Schema uint32 `bson:"_schema"`
	}
	if err := bson.Unmarshal(envelope.Data, &innerMeta); err != nil {
		return nil, 0, err
	}
	return append([]byte(nil), envelope.Data...), innerMeta.Schema, nil
}

var _ entity.AggregateLoader = (*EntityRepository)(nil)
