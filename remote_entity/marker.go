package remote_entity

import (
	"context"
	"github.com/tjbdwanghaibo/cube-core/entity"
	fredis "github.com/tjbdwanghaibo/cube-core/redis"
	"strconv"
	"time"
)

const markerRedisKey = "remote_entity:marks"
const markerFieldTTL = 30 * 24 * time.Hour // 30 days

// redisMarker implements entity.IRemoteEntityMarkerStore using Redis Hash.
type redisMarker struct {
	redis fredis.IRedis
	key   string
}

var _ entity.IRemoteEntityMarkerStore = (*redisMarker)(nil)

func newRedisMarker(redis fredis.IRedis, key string) *redisMarker {
	if key == "" {
		key = markerRedisKey
	}
	return &redisMarker{redis: redis, key: key}
}

// IsMarked checks if entity is currently marked as remote.
func (m *redisMarker) IsMarked(ctx context.Context, id int64) (bool, error) {
	field := strconv.FormatInt(id, 10)
	ok, err := m.redis.HExists(ctx, m.key, field)
	if err != nil {
		return false, err
	}
	return ok, nil
}

// Mark stores mark for entity.
func (m *redisMarker) Mark(ctx context.Context, id int64) error {
	field := strconv.FormatInt(id, 10)
	if err := m.redis.HSet(ctx, m.key, field, "1"); err != nil {
		return err
	}
	// Refresh key TTL
	_, _ = m.redis.Expire(ctx, m.key, markerFieldTTL)
	return nil
}

// Unmark removes mark.
func (m *redisMarker) Unmark(ctx context.Context, id int64) error {
	field := strconv.FormatInt(id, 10)
	_, err := m.redis.HDel(ctx, m.key, field)
	return err
}
