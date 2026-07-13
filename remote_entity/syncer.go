package remote_entity

import (
	"context"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"github.com/tjbdwanghaibo/cube-core/entity"
	"github.com/tjbdwanghaibo/cube-core/replica"
	"log/slog"
)

const syncTopicRemoteEntity = "remote_entity"

// remoteSyncer implements entity.IRemoteEntitySyncer using ISyncBus.
type remoteSyncer struct {
	replicator *replica.Replicator
}

func newRemoteSyncer(replicator *replica.Replicator) *remoteSyncer {
	return &remoteSyncer{replicator: replicator}
}

var _ entity.IRemoteEntitySyncer = (*remoteSyncer)(nil)

func (s *remoteSyncer) SyncEntity(id int64, version int64, collection string, data []byte) error {
	return s.SyncEntityWithContext(fctx.BaseContext(), id, version, collection, data)
}

func (s *remoteSyncer) SyncEntityWithContext(ctx context.Context, id int64, version int64, collection string, data []byte) error {
	if s == nil || s.replicator == nil {
		return nil
	}
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	payload, err := entity.EncodeRemoteSyncPayload(collection, data)
	if err != nil {
		return err
	}
	err = s.replicator.Publish(ctx, replica.Envelope{
		Key:     id,
		Version: version,
		Op:      replica.OpUpsert,
		Payload: payload,
	})
	if err != nil {
		slog.Warn("remote_entity: sync publish failed", "id", id, "err", err)
	}
	return err
}

func (s *remoteSyncer) SyncDelEntity(id int64, version int64) error {
	return s.SyncDelEntityWithContext(fctx.BaseContext(), id, version)
}

func (s *remoteSyncer) SyncDelEntityWithContext(ctx context.Context, id int64, version int64) error {
	if s == nil || s.replicator == nil {
		return nil
	}
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	err := s.replicator.PublishDelete(ctx, id, version)
	if err != nil {
		slog.Warn("remote_entity: sync del publish failed", "id", id, "err", err)
	}
	return err
}

type remoteReplicaStore struct {
	mgr *remoteEntityManager
}

func (s remoteReplicaStore) ApplyReplica(_ context.Context, env replica.Envelope) error {
	if s.mgr == nil || env.Key == 0 {
		return nil
	}
	w, ok := s.mgr.Get(env.Key)
	if env.Op == replica.OpDelete {
		if !ok {
			return nil
		}
		return w.TryDelEntity()
	}
	if !ok {
		meta := entity.ResolveEntityID(env.Key)
		w = s.mgr.getOrCreate(meta.FullID, meta.Category, meta.Kind)
		if w == nil {
			return nil
		}
	}
	rw, ok := w.(*remoteEntityWrapper)
	if ok {
		rw.ensureLoadedForReplica()
	}
	return w.TryUpdateEntity(env.Version, env.Payload)
}

// noopSyncer is a no-op syncer for single-server deployments (no sync bus available).
type noopSyncer struct{}

var _ entity.IRemoteEntitySyncer = (*noopSyncer)(nil)

func (s *noopSyncer) SyncEntity(_ int64, _ int64, _ string, _ []byte) error { return nil }
func (s *noopSyncer) SyncDelEntity(_ int64, _ int64) error                  { return nil }
func (s *noopSyncer) SyncEntityWithContext(context.Context, int64, int64, string, []byte) error {
	return nil
}
func (s *noopSyncer) SyncDelEntityWithContext(context.Context, int64, int64) error { return nil }
