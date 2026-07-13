package remote_entity

import (
	"context"
	fctx "github.com/tjbdwanghaibo/cube-core/ctx"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

var errNoSyncer = errors.New("remote_entity: syncer not configured")

type remoteSyncRetryItem struct {
	collection string
	data       []byte
	id         int64
	version    int64
	attempts   int
	del        bool
}

type syncRetryQueue struct {
	mgr         *remoteEntityManager
	interval    time.Duration
	maxAttempts int
	ch          chan remoteSyncRetryItem
	stopCh      chan struct{}
	done        chan struct{}
	ctx         context.Context
	cancel      context.CancelFunc
	started     atomic.Bool
	stopped     atomic.Bool
	stopOnce    sync.Once
}

func newSyncRetryQueue(mgr *remoteEntityManager, cfg *Config) *syncRetryQueue {
	capacity := cfg.SyncRetryQueueCap
	if capacity <= 0 {
		capacity = 4096
	}
	interval := cfg.SyncRetryInterval
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	return &syncRetryQueue{
		mgr:         mgr,
		interval:    interval,
		maxAttempts: cfg.SyncRetryMaxAttempts,
		ch:          make(chan remoteSyncRetryItem, capacity),
		stopCh:      make(chan struct{}),
		done:        make(chan struct{}),
	}
}

func (q *syncRetryQueue) Start() {
	if q == nil || q.stopped.Load() {
		return
	}
	if q.started.CompareAndSwap(false, true) {
		q.ctx, q.cancel = context.WithCancel(fctx.BaseContext())
		go q.run()
	}
}

func (q *syncRetryQueue) Stop() {
	if err := q.StopWithContext(fctx.BaseContext()); err != nil {
		slog.Warn("remote_entity: sync retry stop failed", "err", err)
	}
}

func (q *syncRetryQueue) StopWithContext(ctx context.Context) error {
	if q == nil {
		return nil
	}
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	if !q.stopped.CompareAndSwap(false, true) {
		if q.started.Load() {
			select {
			case <-q.done:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	if !q.started.Load() {
		return nil
	}
	q.stopOnce.Do(func() {
		if q.cancel != nil {
			q.cancel()
		}
		close(q.stopCh)
	})
	select {
	case <-q.done:
		return nil
	case <-ctx.Done():
		if q.cancel != nil {
			q.cancel()
		}
		return ctx.Err()
	}
}

func (q *syncRetryQueue) Enqueue(item remoteSyncRetryItem) bool {
	if q == nil || q.stopped.Load() {
		return false
	}
	if item.data != nil {
		item.data = append([]byte(nil), item.data...)
	}
	select {
	case q.ch <- item:
		return true
	default:
		slog.Warn("remote_entity: sync retry queue full",
			"id", item.id, "version", item.version, "collection", item.collection, "delete", item.del)
		return false
	}
}

func (q *syncRetryQueue) run() {
	defer close(q.done)
	for {
		select {
		case <-q.stopCh:
			return
		case item := <-q.ch:
			q.retry(q.ctx, item)
		}
	}
}

func (q *syncRetryQueue) retry(ctx context.Context, item remoteSyncRetryItem) {
	if ctx == nil {
		ctx = fctx.BaseContext()
	}
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		if q.maxAttempts > 0 && item.attempts >= q.maxAttempts {
			slog.Warn("remote_entity: sync retry exhausted",
				"id", item.id, "version", item.version, "collection", item.collection, "delete", item.del)
			return
		}
		item.attempts++

		err := q.publish(ctx, item)
		if err == nil {
			return
		}
		slog.Warn("remote_entity: sync retry failed",
			"id", item.id, "version", item.version, "collection", item.collection,
			"delete", item.del, "attempts", item.attempts, "err", err)

		timer := time.NewTimer(q.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-q.stopCh:
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (q *syncRetryQueue) publish(ctx context.Context, item remoteSyncRetryItem) error {
	syncer := q.mgr.syncer
	if syncer == nil {
		return errNoSyncer
	}
	if ctxSyncer, ok := syncer.(remoteEntitySyncerWithContext); ok {
		if item.del {
			return ctxSyncer.SyncDelEntityWithContext(ctx, item.id, item.version)
		}
		return ctxSyncer.SyncEntityWithContext(ctx, item.id, item.version, item.collection, item.data)
	}
	if item.del {
		return syncer.SyncDelEntity(item.id, item.version)
	}
	return syncer.SyncEntity(item.id, item.version, item.collection, item.data)
}

type remoteEntitySyncerWithContext interface {
	SyncEntityWithContext(ctx context.Context, id int64, version int64, collection string, data []byte) error
	SyncDelEntityWithContext(ctx context.Context, id int64, version int64) error
}
